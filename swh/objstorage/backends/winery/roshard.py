# Copyright (C) 2021-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter
import logging
import math
import os.path
import socket
import subprocess
import time
from types import TracebackType
from typing import Callable, Dict, Iterable, Optional, Tuple, Type

from typing_extensions import Literal

from swh.perfecthash import Shard, ShardCreator

from .sharedbase import SharedBase
from .sleep import sleep_exponential
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

    def image_exists(self, image):
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

    def manage_images(
        self,
        base_dsn: str,
        manage_rw_images: bool,
        wait_for_image_factory: Callable[[], Callable[[], None]],
        stop_running: Callable[[], bool],
    ) -> None:
        """Manage RBD image creation and mapping automatically.

        Arguments:
          base_dsn: the DSN of the connection to the SharedBase
          manage_rw_images: whether RW images should be created and mapped
          wait_for_image_factory: returns a `wait_for_image` function which is called
            at each loop iteration, if no images had to be mapped recently
          stop_running: callback that returns True when the manager should stop running
        """
        base = SharedBase(base_dsn=base_dsn)

        mapped_images: Dict[str, Literal["ro", "rw"]] = {}

        wait_for_image = wait_for_image_factory()
        while not stop_running():
            did_something = False
            logger.debug("Listing shards")
            start = time.monotonic()
            shards = list(base.list_shards())
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Listed %d shards in %.02f seconds",
                    len(shards),
                    time.monotonic() - start,
                )
                logger.debug("Mapped images: %s", Counter(mapped_images.values()))

            for shard_name, shard_state in shards:
                mapped_state = mapped_images.get(shard_name)
                if mapped_state == "ro":
                    continue
                elif shard_state.image_available:
                    logger.debug(
                        "Detected %s shard %s, mapping RBD image read-only",
                        shard_state.name,
                        shard_name,
                    )
                    try:
                        self.image_remap_ro(shard_name)
                    except subprocess.CalledProcessError as exc:
                        if exc.returncode == 16:
                            logger.warning(
                                "Could not remap %s shard %s RBD image read-only yet: %s",
                                shard_state.name,
                                shard_name,
                                exc.stderr,
                            )
                        else:
                            raise
                    else:
                        base.record_shard_mapped(
                            name=shard_name, host=socket.gethostname()
                        )
                        mapped_images[shard_name] = "ro"
                        did_something = True
                elif manage_rw_images:
                    if os.path.exists(self.image_path(shard_name)):
                        # Image already mapped, nothing to do
                        pass
                    elif not self.image_exists(shard_name):
                        logger.info(
                            "Detected %s shard %s, creating RBD image",
                            shard_state.name,
                            shard_name,
                        )
                        self.image_create(shard_name)
                        did_something = True
                    else:
                        logger.warn(
                            "Detected %s shard %s and RBD image exists, mapping read-write",
                            shard_state.name,
                            shard_name,
                        )
                        self.image_map(shard_name, "rw")
                        did_something = True
                    # Now the shard is mapped
                    mapped_images[shard_name] = "rw"
                else:
                    logger.debug("%s shard %s, skipping", shard_state.name, shard_name)

            if not did_something:
                # Sleep using the current value
                wait_for_image()
            else:
                # Reset the sleep function
                wait_for_image = wait_for_image_factory()


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
    """Helper for Read-Only shard creation.

    Arguments:
      name: Name of the shard to be initialized
      count: Number of objects to provision in the shard
      rbd_create_images: whether the ROShardCreator should create the rbd
        image, or delegate to the rbd_shard_manager
      rbd_wait_for_image: function called when waiting for a shard to be mapped
      shard_max_size: the size of the shard, passed to :class:`Pool`
      rbd_*: other RBD-related :class:`Pool` arguments
      throttle_*: :class:`Throttler` arguments
    """

    def __init__(
        self,
        name: str,
        count: int,
        rbd_create_images: bool = True,
        rbd_wait_for_image: Callable[[], None] = sleep_exponential(
            min_duration=5,
            factor=2,
            max_duration=60,
            message="Waiting for RBD image mapping",
        )(),
        **kwargs,
    ):
        self.pool = Pool.from_kwargs(**kwargs)
        self.throttler = Throttler(**kwargs)
        self.name = name
        self.count = count
        self.path = self.pool.image_path(self.name)
        self.rbd_create_images = rbd_create_images
        self.rbd_wait_for_image = rbd_wait_for_image

    def __enter__(self) -> "ROShardCreator":
        if self.rbd_create_images:
            self.pool.image_create(self.name)
        else:
            while not os.path.exists(self.path):
                self.rbd_wait_for_image()

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
        if self.rbd_create_images and not exc_type:
            self.pool.image_remap_ro(self.name)

    def add(self, content, obj_id):
        return self.throttler.throttle_add(self.shard.write, obj_id, content)
