# Copyright (C) 2021-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import math
import os
import shutil
import subprocess
from typing import Iterable, Tuple

from swh.shard import Shard, ShardCreator

from . import ImageReader, ImageWriter, Pool
from .. import settings
from ..sleep import sleep_exponential

logger = logging.getLogger(__name__)


class RBDPool(Pool):
    """Manage a Ceph RBD pool for Winery shards.

    Arguments:
      shard_max_size: max size of shard contents
      rbd_use_sudo: whether to use sudo for rbd commands
      rbd_pool_name: name of the pool used for RBD images (metadata)
      rbd_data_pool_name: name of the pool used for RBD images (data)
      rbd_image_features_unsupported: features not supported by the kernel
        mounting the rbd images
      rbd_map_options: options to pass to ``rbd device map``, e.g.
        ``ms_mode=prefer-secure`` to connect to a ceph cluster with encryption
        enabled
    """

    def __init__(
        self,
        shard_max_size: int,
        rbd_use_sudo: bool = True,
        rbd_pool_name: str = "shards",
        rbd_data_pool_name: str | None = None,
        rbd_image_features_unsupported: Tuple[
            str, ...
        ] = settings.DEFAULT_IMAGE_FEATURES_UNSUPPORTED,
        rbd_map_options: str = "",
    ) -> None:
        self.use_sudo = rbd_use_sudo
        self.pool_name = rbd_pool_name
        self.data_pool_name = rbd_data_pool_name or f"{self.pool_name}-data"
        self.features_unsupported = rbd_image_features_unsupported
        self.map_options = rbd_map_options
        self.image_size = math.ceil((shard_max_size * 2) / (1024 * 1024))

    POOL_CONFIG: Tuple[str, ...] = (
        "shard_max_size",
        "rbd_use_sudo",
        "rbd_pool_name",
        "rbd_data_pool_name",
        "rbd_image_features_unsupported",
        "rbd_map_options",
    )

    @classmethod
    def from_kwargs(cls, **kwargs) -> "RBDPool":
        """Create a Pool from a set of arbitrary keyword arguments"""
        return cls(**{k: kwargs[k] for k in cls.POOL_CONFIG if k in kwargs})

    def run(self, *cmd: str) -> Iterable[str]:
        """Run the given command, and return its output as lines.

        Return: the standard output of the run command

        Raises: CalledProcessError if the command doesn't exit with exit code 0.
        """

        sudo = ("sudo",) if self.use_sudo else ()
        cmd = sudo + cmd

        logger.debug(" ".join(repr(item) if " " in item else item for item in cmd))
        result = subprocess.check_output(cmd, encoding="utf-8", stderr=subprocess.PIPE)

        return result.splitlines()

    def rbd(self, *arguments: str) -> Iterable[str]:
        """Run rbd with the given arguments"""

        return self.run("rbd", f"--pool={self.pool_name}", *arguments)

    def image_exists(self, image: str):
        try:
            self.rbd("info", image)
        except subprocess.CalledProcessError:
            return False
        else:
            return True

    def image_list(self):
        try:
            images = self.rbd("ls")
        except subprocess.CalledProcessError as exc:
            if exc.returncode == 2 and "No such file or directory" in exc.stderr:
                return []
            else:
                raise
        return [image.strip() for image in images]

    def image_path(self, image: str) -> str:
        return f"/dev/rbd/{self.pool_name}/{image}"

    def image_create(self, image: str):
        self.rbd(
            "create",
            f"--size={self.image_size}",
            f"--data-pool={self.data_pool_name}",
            image,
        )
        if self.features_unsupported:
            self.rbd(
                "feature",
                "disable",
                f"{self.pool_name}/{image}",
                *self.features_unsupported,
            )
        self.image_map(image, "rw")

    def image_map(self, image: str, options: str):
        self.rbd(
            "device",
            "map",
            "-o",
            f"{options},{self.map_options}" if self.map_options else options,
            image,
        )

    def image_unmap(self, image: str):
        if os.path.exists(self.image_path(image)):
            try:
                self.rbd("device", "unmap", self.image_path(image))
            except subprocess.CalledProcessError as exc:
                if exc.returncode == 22 and "Invalid argument" in exc.stderr:
                    logger.warning(
                        "Image %s already unmapped? stderr: %s", image, exc.stderr
                    )
                else:
                    raise

    def image_open(self, image: str) -> ImageReader:
        return Shard(self.image_path(image))

    def delete_object(self, shard_name: str, obj_id: bytes) -> None:
        Shard.delete(self.image_path(shard_name), obj_id)

    def open_writer(
        self, shard_name: str, nb_objects: int, create_image: bool
    ) -> ImageWriter:
        path = self.image_path(shard_name)
        if create_image:
            self.image_create(shard_name)
        else:
            rbd_wait_for_image = sleep_exponential(
                min_duration=5,
                factor=2,
                max_duration=60,
                message="Waiting for RBD image mapping",
            )
            attempt = 0
            while not os.path.exists(path):
                rbd_wait_for_image(attempt)
                attempt += 1

        self._zero_image_if_needed(path)

        return self._instantiate_writer(path, nb_objects)

    @staticmethod
    def _instantiate_writer(path: str, nb_objects: int) -> ImageWriter:
        return ShardCreator(path, nb_objects)

    def image_import(self, image: str) -> None:
        name = os.path.basename(image)
        dst = self.image_path(name)
        self.image_create(name)
        with open(image, "rb") as s:
            with open(dst, "wb") as d:
                shutil.copyfileobj(s, d)
        self.image_remap_ro(name)
