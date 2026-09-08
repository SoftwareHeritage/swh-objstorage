# Copyright (C) 2021-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
from pathlib import Path
import tempfile
from typing import List, Literal, Optional

import boto3
from botocore.exceptions import ClientError

from swh.mosaic import (
    Backend,
    IdxDescription,
    MosaicCreator,
    MosaicReader,
    MosaicUpdater,
)

from . import FileBackedPool, ImageReader, ImageWriter, Pool

logger = logging.getLogger(__name__)


class MosaicWriterWrapper(MosaicCreator):
    def write(self, key: bytes, object: bytes) -> None:
        super().add([key], object)


class MosaicBackedPool(FileBackedPool):
    """
    swh-mosaic files-backed pool for Winery, mimicking a Ceph RBD pool.
    """

    def __init__(
        self,
        base_directory: Path,
        pool_name: str,
        shard_max_size: int,
        use_permissions: bool = True,
        compression_level: Optional[int] = None,
    ) -> None:
        super().__init__(base_directory, pool_name, shard_max_size, use_permissions)
        self.compression_level = compression_level

    def image_open(self, image: str) -> ImageReader:
        reader = MosaicReader(Path(self.image_path(image)), IdxDescription.SHA256FMPHGO)
        return reader

    def delete_object(self, shard_name, obj_id) -> None:
        path = Path(self.image_path(shard_name))
        index_entry = (IdxDescription.SHA256FMPHGO, obj_id)
        with MosaicUpdater(path) as updater:
            updater.delete([index_entry])

    def open_writer(self, shard_name: str, nb_objects: int) -> ImageWriter:
        path = Path(self.image_path(shard_name))
        # ROShardCreator calls image_create *before* open_writer, but image_create
        # creates an empty file and MosaicCreator requires target file does not exists.
        # For some reason, swh-shard opens with "w+".
        if path.exists():
            # and test_winery_packer_clean_up_interrupted_shard really wants warnings
            logger.warning("cleaning %s", str(path))
            path.unlink()
        return MosaicWriterWrapper(
            path,
            [IdxDescription.SHA256FMPHGO],
            compression_level=self.compression_level,
        )  # type: ignore[return-value]


class MosaicS3BackedPool(Pool):
    """
    swh-mosaic s3-backed pool for Winery.
    """

    def __init__(
        self,
        base_url: str,
        pool_name: str,
        shard_max_size: int,
        compression_level: int | None = None,
        anonymous: bool = False,
        read_only: bool = False,
        image_extension: str = "",
        tmp_dir: str | None = None,
        boto3_config=None,
    ) -> None:
        self.pool_name = pool_name
        assert base_url.startswith("s3://")
        base_url = base_url.removeprefix("s3://").rstrip("/")

        if "/" in base_url:
            bucket, prefix = base_url.split("/", 1)
        else:
            bucket, prefix = base_url, ""

        self.bucket = bucket
        if prefix:
            self.prefix = f"{prefix}/{pool_name}"
        else:
            self.prefix = pool_name
        self.base_url = f"s3://{self.bucket}/{self.prefix}"
        self.image_size = shard_max_size
        self.read_only = read_only

        if read_only:
            self.tmp_dir = None
        else:
            if tmp_dir is None:
                tmp_dir = tempfile.mkdtemp(prefix="mosaic-pool-")
            self.tmp_dir = Path(tmp_dir)
            if not self.tmp_dir.is_dir() or not os.access(
                self.tmp_dir, os.R_OK | os.W_OK | os.X_OK
            ):
                raise EnvironmentError(
                    f"Temporary directory {self.tmp_dir} does not exists "
                    "or is not accessible"
                )
            (self.tmp_dir / self.pool_name).mkdir(exist_ok=True)
            if not os.access(
                self.tmp_dir / self.pool_name, os.R_OK | os.W_OK | os.X_OK
            ):
                raise EnvironmentError(
                    f"Temporary directory {self.tmp_dir/self.pool_name} "
                    "is not accessible"
                )

        # for now credentials need to be specified via env vars (esp.
        # AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY) for the mosaic s3
        # backend
        if boto3_config is None:
            boto3_config = {}

        self.client = boto3.client("s3", **boto3_config)

        self.compression_level = compression_level
        self.extension = image_extension

    def image_path(self, image: str) -> str:
        if self.image_mapped(image) == "ro":
            return f"{self.base_url}/{image}{self.extension}"
        else:
            assert self.tmp_dir is not None
            return f"{self.tmp_dir / self.pool_name / (image + self.extension)}"

    def image_list(self) -> List[str]:
        # TODO: boto3 client.list_objects will only return up to 1000 entries, need to loop
        return [
            entry["Key"]
            .removesuffix(self.extension)
            .removeprefix(self.prefix)
            .strip("/")
            for entry in self.client.list_objects(
                Bucket=self.bucket, Prefix=self.prefix
            )["Contents"]
        ]

    def image_mapped(self, image: str) -> Optional[Literal["ro", "rw", "any"]]:
        if (
            self.tmp_dir
            and (self.tmp_dir / self.pool_name / f"{image}{self.extension}").is_file()
        ):
            return "rw"
        try:
            self.client.head_object(
                Bucket=self.bucket, Key=f"{self.prefix}/{image}{self.extension}"
            )
            return "ro"
        except ClientError:
            return None

    def image_create(self, image: str) -> None:
        if self.read_only:
            raise EnvironmentError("Read-only pool!")

        assert self.tmp_dir is not None
        imgpath = self.tmp_dir / self.pool_name / (image + self.extension)
        imgpath.touch()

    def image_map(self, image: str, options: str) -> None:
        assert self.tmp_dir is not None
        imgpath = self.tmp_dir / self.pool_name / f"{image}{self.extension}"
        if options == "ro":
            if self.image_mapped(image) == "rw":
                assert self.tmp_dir is not None
                if not imgpath.is_file():
                    raise EnvironmentError(
                        f"Missing local copy of the mosaic file in {imgpath}"
                    )
                # means we have the local copy of the mosaic, not yet uploaded
                # on s3, so let's do it
                # TODO: handle hard put failure
                self.client.upload_file(
                    Bucket=self.bucket,
                    Key=f"{self.prefix}/{image}{self.extension}",
                    Filename=str(imgpath),
                )
                imgpath.unlink()
        if options == "rw":
            if self.image_mapped(image) == "ro":
                self.client.download_file(
                    Bucket=self.bucket,
                    Key=f"{self.prefix}/{image}{self.extension}",
                    Filename=str(imgpath),
                )

    def image_unmap(self, image: str) -> None:
        pass

    def image_unmap_all(self) -> None:
        pass

    def image_open(self, image: str) -> ImageReader:
        imgpath = self.image_path(image)
        if self.image_mapped(image) == "ro":
            backend = Backend.S3
        else:
            backend = Backend.MMAP
        reader = MosaicReader(imgpath, IdxDescription.SHA256FMPHGO, backend)
        return reader

    def delete_object(self, image: str, obj_id: bytes) -> None:
        assert self.tmp_dir is not None
        imgpath = self.tmp_dir / self.pool_name / f"{image}{self.extension}"

        if self.image_mapped(image) == "rw":
            index_entry = (IdxDescription.SHA256FMPHGO, obj_id)
            with MosaicUpdater(imgpath) as updater:
                updater.delete([index_entry])

    def open_writer(self, shard_name: str, nb_objects: int) -> ImageWriter:
        assert self.tmp_dir is not None
        imgpath = self.tmp_dir / self.pool_name / f"{shard_name}{self.extension}"
        # ROShardCreator calls image_create *before* open_writer, but image_create
        # creates an empty file and MosaicCreator requires target file does not exists.
        # For some reason, swh-shard opens with "w+".
        if imgpath.exists():
            # and test_winery_packer_clean_up_interrupted_shard really wants warnings
            logger.warning("cleaning %s", str(imgpath))
            imgpath.unlink()
        return MosaicWriterWrapper(
            imgpath,
            [IdxDescription.SHA256FMPHGO],
            compression_level=self.compression_level,
        )  # type: ignore[return-value]

    def image_import(self, image_path: str) -> None:
        image = os.path.splitext(os.path.basename(image_path))[0]
        self.client.upload_file(
            Bucket=self.bucket,
            Key=f"{self.prefix}/{image}{self.extension}",
            Filename=image_path,
        )
