# Copyright (C) 2021-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import math
import os.path
import subprocess
from types import TracebackType
from typing import Iterable, Optional, Tuple, Type

from swh.perfecthash import Shard, ShardCreator

from .throttler import Throttler

logger = logging.getLogger(__name__)

# This would be used for image features that are not supported by the kernel RBD
# driver, e.g. exclusive-lock, object-map and fast-diff for kernels < 5.3
DEFAULT_IMAGE_FEATURES_UNSUPPORTED: Tuple[str, ...] = ()


class Pool(object):
    """Manage a Ceph RBD pool for Winery shards.

    Arguments:
      shard_max_size: max size of shard contents
      rbd_use_sudo: whether to use sudo for rbd commands
      rbd_pool_name: name of the pool used for RBD images (metadata)
      rbd_data_pool_name: name of the pool used for RBD images (data)
      rbd_image_features_unsupported: features not supported by the kernel
        mounting the rbd images
    """

    def __init__(
        self,
        shard_max_size: int,
        rbd_use_sudo: bool = True,
        rbd_pool_name: str = "shards",
        rbd_data_pool_name: Optional[str] = None,
        rbd_image_features_unsupported: Tuple[
            str, ...
        ] = DEFAULT_IMAGE_FEATURES_UNSUPPORTED,
    ) -> None:
        self.use_sudo = rbd_use_sudo
        self.pool_name = rbd_pool_name
        self.data_pool_name = rbd_data_pool_name or f"{self.pool_name}-data"
        self.features_unsupported = rbd_image_features_unsupported
        self.image_size = math.ceil((shard_max_size * 2) / (1024 * 1024))

    POOL_CONFIG: Tuple[str, ...] = (
        "shard_max_size",
        "rbd_use_sudo",
        "rbd_pool_name",
        "rbd_data_pool_name",
        "rbd_image_features_unsupported",
    )

    @classmethod
    def from_kwargs(cls, **kwargs) -> "Pool":
        """Create a Pool from a set of arbitrary keyword arguments"""
        return cls(**{k: kwargs[k] for k in cls.POOL_CONFIG if k in kwargs})

    def run(self, *cmd: str) -> Iterable[str]:
        """Run the given command, and return its output as lines.

        Return: the standard output of the run command

        Raises: CalledProcessError if the command doesn't exit with exit code 0.
        """

        sudo = ("sudo",) if self.use_sudo else ()
        cmd = sudo + cmd

        logger.info(" ".join(repr(item) if " " in item else item for item in cmd))
        result = subprocess.check_output(cmd, encoding="utf-8", stderr=subprocess.PIPE)

        return result.splitlines()

    def rbd(self, *arguments: str) -> Iterable[str]:
        """Run rbd with the given arguments"""

        return self.run("rbd", f"--pool={self.pool_name}", *arguments)

    def image_list(self):
        try:
            images = self.rbd("ls")
        except subprocess.CalledProcessError as exc:
            if exc.returncode == 2 and "No such file or directory" in exc.stderr:
                return []
            else:
                raise
        return [image.strip() for image in images]

    def image_path(self, image):
        return f"/dev/rbd/{self.pool_name}/{image}"

    def image_create(self, image):
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

    def image_map(self, image, options):
        self.rbd("device", "map", "-o", options, image)
        self.run("chmod", "777", self.image_path(image))

    def image_remap_ro(self, image):
        self.image_unmap(image)
        self.image_map(image, "ro")

    def image_unmap(self, image):
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


class ROShard:
    def __init__(self, name, **kwargs):
        self.pool = Pool.from_kwargs(**kwargs)
        self.throttler = Throttler(**kwargs)
        self.name = name
        self.path = self.pool.image_path(self.name)
        self.shard = Shard(self.path)
        logger.debug("ROShard %s: loaded", self.name)

    def get(self, key):
        return self.throttler.throttle_get(self.shard.lookup, key)


class ROShardCreator:
    def __init__(self, name: str, count: int, **kwargs):
        self.pool = Pool.from_kwargs(**kwargs)
        self.throttler = Throttler(**kwargs)
        self.name = name
        self.count = count
        self.path = self.pool.image_path(self.name)

    def __enter__(self) -> "ROShardCreator":
        self.pool.image_create(self.name)
        self.shard = ShardCreator(self.path, self.count)
        logger.debug("ROShard %s: created", self.name)
        self.shard.__enter__()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.shard.__exit__(exc_type, exc_val, exc_tb)
        if not exc_type:
            self.pool.image_remap_ro(self.name)

    def add(self, content, obj_id):
        return self.throttler.throttle_add(self.shard.write, obj_id, content)
