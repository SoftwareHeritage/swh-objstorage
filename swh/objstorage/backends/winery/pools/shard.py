# Copyright (C) 2021-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from swh.shard import Shard, ShardCreator

from . import FileBackedPool, ImageReader, ImageWriter

logger = logging.getLogger(__name__)


class ShardBackedPool(FileBackedPool):
    """
    swh-shard files-backed pool for Winery.
    """

    def image_open(self, image: str) -> ImageReader:
        return Shard(self.image_path(image))

    def delete_object(self, shard_name: str, obj_id: bytes) -> None:
        Shard.delete(self.image_path(shard_name), obj_id)

    def open_writer(
        self, shard_name: str, nb_objects: int, create_image: bool
    ) -> ImageWriter:
        path = self.image_path(shard_name)
        self.image_create(shard_name)
        self._zero_image_if_needed(path)

        return ShardCreator(path, nb_objects)
