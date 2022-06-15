import botocore.exceptions
import lightning as L
from lightning.storage.payload import Payload
import boto3
import logging
from typing import final, Union, Optional
from torch.utils.data import Dataset, DataLoader
from PIL import Image
import io


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
            filename: Union[L.storage.Path, str],
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
         self, bucket: str, object: str, filename: Union[L.storage.Path, str]
    ):
        with open(filename, "wb") as _file:
            self.resource.meta.client.download_fileobj(
                Bucket=bucket, Key=object, Fileobj=_file
            )

    def upload_file(
            self,
            bucket: str,
            filename: Union[L.storage.Path, str],
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
            filename: Union[L.storage.Path, str]
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

    def get_s3_items(data, idx, transform):
        obj = data[idx]
        label = obj.key.split('/')[-2]
        img_bytes = obj.get()['Body'].read()
        img = Image.open(io.BytesIO(img_bytes)).convert('RGB')
        # Apply preprocessing functions on data
        if transform is not None:
             img = self.transform(img)
        return img, label

    def create_dataset(
        self,
        bucket,
        transform=None,
        get_s3_items=get_s3_items
    ):
        resource=self.resource
        class S3Dataset(Dataset):
            def __init__(self, bucket, transform=None, split='train'):
                self.transform = transform
                # Check that the bucket exists, if not raise a warning
                self.data = [obj for obj in resource.Bucket(bucket).objects.all() if obj.key.split('/')[1].lower() == split.lower()]

            def __len__(self):
                return len(self.data)

            def __getitem__(self, idx):
                return get_s3_items(self.data, idx, self.transform)

        return S3Dataset(bucket, transform)
