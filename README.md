> **Warning**
>
> #### EXPERIMENTAL

<!---:lai-name: BigQuery--->

<div align="center">
<img src="static/s3-icon.svg" width="200px">

```
A Lightning component to move files from and to Amazon S3.
______________________________________________________________________
```

[![Lightning](https://img.shields.io/badge/-Lightning-792ee5?logo=pytorchlightning&logoColor=white)](https://lightning.ai)
![license](https://img.shields.io/badge/License-Apache%202.0-blue.svg)
[![CI testing](https://github.com/Lightning-Universe/AWS-s3_component/actions/workflows/ci-testing.yml/badge.svg?event=push)](https://github.com/Lightning-Universe/AWS-s3_component/actions/workflows/ci-testing.yml)

</div>

### About

This component lets you upload and download files to Amazon S3.

### Use the component

To download a file or list files in a bucket

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
            bucket="YOUR BUCKET NAME",
            object="REMOTE 'PATH' TO FILE",
            filename="NAME OF DOWNLOADED FILE",
        )

        # Retrieves "filenames" from a bucket and stores it in the `data` attr
        self.s3.list_files("YOUR BUCKET NAME")
        # Print the file
        print(self.s3.data)


app = L.LightningApp(LitApp())
```

### Install

Run the following to install:

```shell
git clone https://github.com/PyTorchLightning/LAI-s3
cd LAI-s3
pip install -r requirements.txt
pip install -e .
```

### Tests

To run unit tests locally:

```shell
# From the root level of the package (LAI-bigquery)
pip install -r tests/requirements.txt
pytest
```
