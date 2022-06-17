import lightning as L

from lai_s3 import S3


class LitApp(L.LightningFlow):
    """Sample App for Testing."""

    def __init__(self) -> None:
        super().__init__()
        self.lai_s3 = S3()

    def run(self):
        self.lai_s3.get_filelist("lightningapps")
        print(self.lai_s3.data)
        self._exit()


app = L.LightningApp(LitApp())
