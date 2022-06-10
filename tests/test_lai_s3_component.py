r"""
To test a lightning component:

1. Init the component.
2. call .run()
"""
import os
import unittest
from unittest.mock import patch
import pathlib

import botocore
import boto3
import lightning as L
from lightning.storage.path import Path
from lai_s3.s3 import S3


class MockSession:

    def __init__(self, service):
        self.service = service

    def client(self, service):
        return MockClient(service)

    def resource(self, service):
        return MockResource(service)

class Object:

    def __init__(self, key):
        self.key = key

class MockObjects():

    def all(self):
        return [
            Object("foo"), Object("bar")
        ]

class MockBucket:
    def __init__(self, bucket):
        self.bucket = bucket
        self.objects = MockObjects()


class MockClient:

    def __init__(self, service):
        self.service = service

    def get_caller_identity(self):
        if self.service == "sts":
            raise botocore.exceptions.ClientError(
                {"Erroc": "fake error"}, "error"
            )

    @property
    def meta(self):
        return MockMeta()

    def download_fileobj(Bucket, Key, Fileobj):
        Fileobj.write(b"hello")

class MockMeta:
    client = MockClient
    def service_model(self):
        pass

class MockResource:

    def __init__(self, service):
        self.service = service
        self.meta = MockMeta

    def Bucket(self, bucket):
        return MockBucket(bucket)



class TestCredentials(unittest.TestCase):

    def test_missing_access_key_id(self):
        aws_access_key_id = None
        aws_secret_access_key = "foo"
        with patch.object(
                boto3.session.Session, "client", return_value=MockClient("sts")
        ) as _:
            self.assertRaises(
                PermissionError, S3, aws_access_key_id, aws_secret_access_key
            )

    def test_missing_secret_access_key(self):
        aws_access_key_id = "foo"
        aws_secret_access_key = None
        with patch.object(
                boto3.session.Session, "client", return_value=MockClient("sts")
        ) as _:
            self.assertRaises(
                PermissionError, S3, aws_access_key_id, aws_secret_access_key
            )

class MockS3(S3):

    def run(self, *args, **kwargs):
        with patch.object(
            boto3.session, "Session", return_value=MockSession("sts")
        ) as _, \
        patch.object(
            boto3.session.Session, "client", return_value=MockClient("sts")
        ) as _ :
            super().run(*args, **kwargs)


class S3Interface(L.LightningFlow):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        self.s3 = MockS3(aws_access_key_id, aws_secret_access_key)
        self.passed_list = False
        self.passed_download = False

        self.local_file = "test.txt"
        self.uploaded_object = "dead3.jpg"
        self.object_to_download = "dead.jpg"


    def run(self):

        if self.passed_list is False:
            self.s3.get_filelist("lightningapps")

        elif self.passed_download is False:
            self.s3.download_file(
                bucket="lightningapps",
                object=self.object_to_download,
                filename=self.local_file
            )
            myfile = Path(self.local_file)
            myfile.touch()

        # Blocks each run of the work so that these can run sequentially.
        if not self.s3.has_succeeded:
            return

        # Test cases:
        if self.passed_list is False:
            assert self.s3.data.__len__() > 0
            assert self.s3.data == {'lightningapps': ['foo', 'bar']}
            self.passed_list = True
            return
        elif self.passed_download is False:
            assert os.path.exists(self.local_file)
            os.remove(self.local_file)
            self.passed_download = True

        assert self.passed_download is True and \
               self.passed_list is True

        # Exit when all tests finished
        self._exit()

def test_insert_from_app():
    app = L.LightningApp(S3Interface(), debug=True)
    L.runners.MultiProcessRuntime(app, start_server=False).dispatch()
