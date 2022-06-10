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
from lai_s3.s3 import S3


class LitApp(L.LightningFlow):
    def __init__(self) -> None:
        super().__init__()
        self.s3 = S3()

    def run(self):
        self.s3.list_files(<BUCKET_NAME>)
        # Print the file
        self.s3.data
        
        self.s3


app = L.LightningApp(LitApp())
```
