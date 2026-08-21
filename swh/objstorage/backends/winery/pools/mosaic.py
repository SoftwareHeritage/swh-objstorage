# Copyright (C) 2021-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from pathlib import Path
from typing import Optional

from swh.mosaic import IdxDescription, MosaicCreator, MosaicReader, MosaicUpdater

from . import FileBackedPool, ImageReader, ImageWriter

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
