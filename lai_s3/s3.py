import botocore.exceptions
import lightning as L
import lightning_app as lapp
import boto3
import logging
from typing import final, Union, Optional
from torch.utils.data import Dataset, DataLoader
from PIL import Image
import io
import json


class S3(L.LightningWork):

    def __init__(
            self,
            aws_access_key_id=None,
            aws_secret_access_key=None,
            aws_session_token=None,
            *args, **kwargs
    ):
        super().__init__(self, *args, **kwargs)

        self.data = {} # Bucket name / bucket contents
        self.credentials = {
            'aws_access_key_id': aws_access_key_id,
            'aws_secret_access_key': aws_secret_access_key,
            'aws_session_token': aws_session_token
        }
        self.verify_credentials()

    def verify_credentials(self):
        missing_creds = []
        for cred in self.credentials:
            if self.credentials[cred] is None:
                missing_creds += self.credentials[cred]

        if missing_creds:
            raise PermissionError(
                "If either the aws_access_key_id, aws_secret_access_key, or aws_session_token is"
                " provided then all credential fields are required."
                f" Missing value for {missing_creds}"
            )

        elif not self.credentials['aws_access_key_id'] and not self.credentials['aws_secret_access_key']:
            logging.info("Using default credentials from .aws")

        # Verify that the access key pairs are valid
        try:
            self._session.client("sts").get_caller_identity()
        except botocore.exceptions.ClientError as error:
            logging.error(error)


    @property
    def _session(self):
        return boto3.session.Session(
            aws_access_key_id=self.credentials['aws_access_key_id'],
            aws_secret_access_key=self.credentials['aws_secret_access_key'],
            aws_session_token=self.credentials['aws_session_token']
        )

    @property
    def resource(self):
        return self._session.resource("s3")

    def get_filelist(self, bucket, *args, **kwargs) -> None:
        self.run(action="get_filelist", bucket=bucket, *args, **kwargs)

    @final
    def _get_filelist(self, bucket) -> None:

        # Check that the bucket exists, if not raise a warning
        content = [
            _obj.key for _obj in self.resource.Bucket(bucket).objects.all()
        ]
        self.data = {**{bucket: content}, **self.data}

    def download_file(
            self,
            bucket: str,
            object: str,
            filename: Union[lapp.storage.Path, str],
            *args,
            **kwargs
    ) -> None:

        self.run(
            action="download_file",
            bucket=bucket,
            object=object,
            filename=filename,
            *args,
            **kwargs
        )

    @final
    def _download_file(
         self, bucket: str, object: str, filename: Union[lapp.storage.Path, str]
    ):
        with open(filename, "wb") as _file:
            self.resource.meta.client.download_fileobj(
                Bucket=bucket, Key=object, Fileobj=_file
            )

    def upload_file(
            self,
            bucket: str,
            filename: Union[lapp.storage.Path, str],
            object: Optional[str] = None,
            *args,
            **kwargs
    ):
        self.run(
            action="upload_file",
            bucket=bucket,
            object=object,
            filename=filename,
            *args,
            **kwargs
        )

    @final
    def _upload_file(
            self,
            bucket: str,
            object: str,
            filename: Union[lapp.storage.Path, str]
    ):
        with open(filename, 'rb') as _f:
            self.resource.meta.client.upload_fileobj(
                Fileobj=_f, Bucket=bucket, Key=object
            )

    def run(self, action, *args, **kwargs):
        if action == "get_filelist":
            self._get_filelist(*args, **kwargs)
        elif action == "download_file":
            self._download_file(*args, **kwargs)
        elif action == "upload_file":
            self._upload_file(*args, **kwargs)
        elif action == 'create_dataset':
            return self._create_dataset(*args, **kwargs)


    def __getitem__override(data, idx, transforms=None, data_type='img', label_map=None):
        if data_type.lower() == 'img':
            obj = data[idx]
            label = obj.key.split('/')[-2]
            label = label_map[label]
            img_bytes = obj.get()['Body'].read()
            img = Image.open(io.BytesIO(img_bytes)).convert('RGB')
            out = [img, label]
        elif data_type.lower() == 'tabular':
            columns = data.columns
            features = data.loc[idx, columns[0:-1]].values
            label = df.at[idx, columns[-1]]
            out = [features, label]
        else:
            raise("This component only supports an img or tabular data_type")

        # Apply preprocessing functions on data
        if transforms is not None:
            out[0] = transforms(out[0])
        return out
    
    def create_dataset(
        self,
        bucket,
        data_type='img',
        label_map='labels-mapping.json',
        transforms=None,
        __getitem__override=__getitem__override,
        split='train'
    ):
        '''
        The create_dataset is a helper function used to create a custom torch dataset capable of loading 
        data from a private S3 bucket. By default it has strict expectations for the directory organization 
        and the image type. It expects the data to be image type and for the directory structure to be something
        aking to:
        
        train\
            class 1\
                imgs
                ...
            class 2\
                imgs
                ...
        test\
            ...
        val\
            ....
        '''
        self.run(
            action='create_dataset',
            bucket=bucket, 
            data_type=data_type,
            label_map='labels-mapping.json',
            transforms=transforms,
            __getitem__override=__getitem__override
                 
    def _create_dataset(
        self,
        bucket,
        data_type='img',
        label_map='labels-mapping.json',
        transforms=None,
        _get_s3_items=_get_s3_items,
        split='train'
    ):
        resource=self.resource
        class S3Dataset(Dataset):
            '''
            The S3Dataset class creates a custom Pytorch Dataset for you. Based on the current implementation the
            __getitem__ method is overrideable by passing in it in for __getitem__override in the create_dataset method.
            '''
            def __init__(self, bucket, transforms=None, split=split):
                self.transforms = transforms
                # Check that the bucket exists, if not raise a warning
                self.data = [
                    obj for obj in resource.Bucket(bucket).objects.all() if label_map not in obj.key and obj.key.split('/')[1].lower() == split.lower()
                    ]

                # get label_map in default case
                if label_map is not None:
                    try:
                        json_bytes = resource.meta.client.get_object(Bucket=bucket, Key=label_map)['Body'].read()
                        json_data = json_bytes.decode('utf8').replace("'", '"')
                        self.label_map = json.loads(json_data)
                    except json.JSONDecodeError:
                        print("Error decoding json containing image mappings. Does your bucket contain one?")
                else:
                    self.label_map = None

            def __len__(self):
                return len(self.data)

            def __getitem__(self, idx):
                return _get_s3_items(data=self.data, idx=idx, transforms=self.transforms, label_map=self.label_map)

        return S3Dataset(bucket, transforms)
