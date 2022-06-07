import lightning as L
import boto3.session

class S3(L.LightningWork):

    def __init__(
            self,
            aws_access_key_id,
            aws_secret_access_Key,
            *args, **kwargs
    ):
        super().__init__(self, *args, **kwargs)

        # Metadata cache to keep track of what's been downloaded and where
        # Key/Value pair of Remote s3 bucket uri / lightning storage path
        self.metadata = {}
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_Key = aws_secret_access_Key

    @property
    def session(self):
        return boto3.session.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_Key
        )

    def ls(self, bucket, *args, **kwargs) -> None:
        self.run(bucket, action="ls", *args, **kwargs)

    def _ls(self, bucket) -> None:
        s3 = self.session.resource('s3')
        print(s3.Bucket(bucket).objects.all())

    def download(self):
        self.run(action="download")

    def _download(self):
        pass

    def upload(self):
        self.run(action="upload")

    def _upload(self):
        pass

    def create_bucket(self):
        self.run(action="create_bucket")

    def _create_bucket(self):
        pass

    def list_bucket(self):
        self.run(action="list_bucket")

    def _list_bucket(self):
        pass

    def run(self, action, *args, **kwargs):
        if action == "ls":
            self._ls(*args, **kwargs)
        elif action == "download":
            self._download(*args, **kwargs)
        elif action == "upload":
            self._upload(*args, **kwargs)
        elif action == "create_bucket":
            self._create_bucket(*args, **kwargs)
        elif action == "list_buckets":
            self._list_buckets(*args, **kwargs)