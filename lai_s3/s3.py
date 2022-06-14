import botocore.exceptions
import lightning as L
from lightning.storage.payload import Payload
import boto3
import logging
from typing import final, Union, Optional
from torch.utils.data import Dataset, DataLoader
import io
from pil import Image


class S3(L.LightningWork):

    def __init__(
            self,
            aws_access_key_id,
            aws_secret_access_key,
            *args, **kwargs
    ):
        super().__init__(self, *args, **kwargs)

        self.data = {} # Bucket name / bucket contents
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.verify_credentials()

    def verify_credentials(self):

        if sum([
                self.aws_access_key_id is None,
                self.aws_secret_access_key is None
        ]) == 1:
            missing_key = 'aws_access_key_id' if self.aws_secret_access_key \
                else 'aws_secret_access_key'
            raise PermissionError(
                "If either the aws_access_key_id or aws_secret_access_key is"
                " provided then both are required."
                f" Missing value for {missing_key}"
            )

        elif not self.aws_access_key_id and not self.aws_secret_access_key:
            logging.info("Using default credentials from .aws")

        # Verify that the access key pairs are valid
        try:
            self._session.client("sts").get_caller_identity()
        except botocore.exceptions.ClientError as error:
            logging.error(error)


    @property
    def _session(self):
        return boto3.session.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
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

    def get_s3_items(self, idx):
        filepath = self.data[idx]
        img = Image.open(io.BytesIO(img)).convert('RGB')
        # Apply preprocessing functions on data
        if self.transform is not None:
             img = self.transform(img)
        return img

    def create_dataset(
        self,
        transform,
        get_s3_items=self.get_s3_items
    ):
        class S3Dataset(Dataset):
            def __init__(bucket, transform=None, resource=None):
                self.bucket = bucket
                self.resource = resource
                self.transform = transform
                # Check that the bucket exists, if not raise a warning
                self.data = [
                    _obj.key for _obj in self.resource.Bucket(bucket).objects.all()
                ]

            def __len__(self):
                return len(self.data)


            def __getitem__(self, idx):
                return get_s3_items(idx)

        return S3Dataset(Dataset, transform, self.resource)
