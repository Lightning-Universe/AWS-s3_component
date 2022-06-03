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
from lai_s3 import TemplateComponent
import lightning as L


class LitApp(L.LightningFlow):
    def __init__(self) -> None:
        super().__init__()
        self.lai_s3 = TemplateComponent()

    def run(self):
        print(
            "this is a simple Lightning app to verify your component is working as expected"
        )
        self.lai_s3.run()


app = L.LightningApp(LitApp())
```
