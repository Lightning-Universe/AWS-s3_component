import os
from typing import Optional
from lai_s3 import S3
import torchvision
from torch.utils.data import DataLoader
import pytorch_lightning as pl
from pl_bolts.transforms.dataset_normalizations import cifar10_normalization
from pytorch_lightning import Trainer, seed_everything
from pytorch_lightning.callbacks import LearningRateMonitor
from pytorch_lightning.loggers import TensorBoardLogger
from model import create_model, LitResnet, BATCH_SIZE, AVAIL_GPUS


seed_everything(7)
AVAIL_GPUS = min(1, torch.cuda.device_count())
BATCH_SIZE = 256 if AVAIL_GPUS else 64
NUM_WORKERS = int(os.cpu_count() / 2)

train_transforms = torchvision.transforms.Compose(
    [
        torchvision.transforms.RandomCrop(32, padding=4),
        torchvision.transforms.RandomHorizontalFlip(),
        torchvision.transforms.ToTensor(),
        cifar10_normalization(),
    ]
)

test_transforms = torchvision.transforms.Compose(
    [
        torchvision.transforms.ToTensor(),
        cifar10_normalization(),
    ]
)


class CifarDataModule(pl.LightningDataModule):
    def __init__(self, bucket, aws_access_key_id, aws_secret_access_key, aws_session_token, transforms=None, batch_size: int = 32):
        super().__init__()
        self.batch_size = batch_size
        self.bucket = bucket
        self.s3 = S3(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token
        )
        self.transforms = transforms


    def setup(self, stage: Optional[str] = None):
        self.test = self.s3.run(action='create_dataset', bucket=self.bucket, split='test', transforms=self.transforms)
        self.train = self.s3.run(action='create_dataset', bucket=self.bucket, split='train', transforms=self.transforms)

    def train_dataloader(self):
        return DataLoader(self.train, batch_size=self.batch_size)

    def test_dataloader(self):
        return DataLoader(self.test, batch_size=self.batch_size)

    def predict_dataloader(self):
        return DataLoader(self.test, batch_size=self.batch_size)

    def teardown(self, stage: Optional[str] = None):
        # Used to clean-up when the run is finished
        pass


cifar10_dm = CifarDataModule(
    bucket="",
    aws_access_key_id="",
    aws_secret_access_key="",
    aws_session_token="",
    transforms=test_transforms
)
model = LitResnet(lr=0.05)
model.datamodule = cifar10_dm

trainer = Trainer(
    progress_bar_refresh_rate=10,
    max_epochs=30,
    gpus=AVAIL_GPUS,
    logger=TensorBoardLogger("lightning_logs/", name="resnet"),
    callbacks=[LearningRateMonitor(logging_interval="step")],
)

trainer.fit(model, cifar10_dm)
trainer.test(model, datamodule=cifar10_dm)
