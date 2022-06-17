# lai_s3 component

This ⚡ [Lightning component](lightning.ai) ⚡ was generated automatically with:

```bash
lightning init component lai_s3
```

## To run lai_s3

First, install lai_s3 (warning: this app has not been officially approved on the lightning gallery):

```bash
lightning install component https://github.com/theUser/lai_s3
```

Once the app is installed, use it in an app:

```python
import lightning as L
from lai_s3.s3 import S3


class LitApp(L.LightningFlow):
    def __init__(self) -> None:
        super().__init__()
        self.s3 = S3("YOUR AWS ACCESS KEY ID", "YOUR AWS SECRET ACCESS KEY")

    def run(self):
        # Download file
        self.s3.download_file(
            bucket="YOUR BUCKET NAME", object="REMOTE 'PATH' TO FILE", filename="NAME OF DOWNLOADED FILE"
        )

        # Retrieves "filenames" from a bucket and stores it in the `data` attr
        self.s3.list_files("YOUR BUCKET NAME")
        # Print the file
        self.s3.data


app = L.LightningApp(LitApp())
```
