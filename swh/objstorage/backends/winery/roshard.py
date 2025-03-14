# Copyright (C) 2021-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter
import logging
import math
import os
from pathlib import Path
import random
import shlex
import socket
import stat
import subprocess
import time
from types import TracebackType
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Protocol,
    Tuple,
    Type,
)

from systemd.daemon import notify

from swh.perfecthash import Shard, ShardCreator

from . import settings
from .sharedbase import ShardState, SharedBase
from .sleep import sleep_exponential
from .throttler import Throttler

logger = logging.getLogger(__name__)


class ShardNotMapped(Exception):
    pass


class Pool(Protocol):
    def image_exists(self, image: str) -> bool:
        """Check whether the named image exists (it does not have to be mapped)"""
        ...

    def image_mapped(self, image: str) -> Optional[Literal["ro", "rw"]]:
        """Check whether the image is already mapped, read-only or read-write"""
        try:
            image_stat = os.stat(self.image_path(image))
        except FileNotFoundError:
            return None
        return "rw" if (image_stat.st_mode & 0o222) != 0 else "ro"

    def image_list(self) -> List[str]:
        """List all known images, mapped or not"""
        ...

    def image_path(self, image: str) -> str:
        """Return a path to the image, that can be opened with :func:`open`."""
        ...

    def image_create(self, image: str) -> None:
        """Create a new image named `image` and allocate the right amount of space."""
        ...

    def image_map(self, image: str, options: str) -> None:
        """Map an image for use. Options can be `"ro"` to map the image read-only, or
        `"rw"` to map the image read-write."""
        ...

    def image_unmap(self, image: str) -> None:
        """Unmap the image. Once this is done, the image is unavailable for use."""
        ...

    def image_remap_ro(self, image: str):
        self.image_unmap(image)
        self.image_map(image, "ro")


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
        rbd_data_pool_name: Optional[str] = None,
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


class FileBackedPool(Pool):
    """File-backed pool for Winery shards mimicking a Ceph RBD pool.

    Unmapped images are represented by setting the file permission to 0o000.
    """

    def __init__(
        self,
        base_directory: Path,
        pool_name: str,
        shard_max_size: int,
    ) -> None:
        self.base_directory = base_directory
        self.pool_name = pool_name
        self.image_size = shard_max_size

        self.pool_dir = self.base_directory / self.pool_name
        self.pool_dir.mkdir(exist_ok=True)

    def image_exists(self, image: str) -> bool:
        return (self.pool_dir / image).is_file()

    def image_list(self) -> List[str]:
        return [entry.name for entry in self.pool_dir.iterdir() if entry.is_file()]

    def image_path(self, image: str) -> str:
        return str(self.pool_dir / image)

    def image_create(self, image: str) -> None:
        path = self.image_path(image)
        if os.path.exists(path):
            raise ValueError(f"Image {image} already exists")
        open(path, "w").close()
        os.truncate(path, self.image_size * 1024 * 1024)
        self.image_map(image, "rw")

    def image_map(self, image: str, options: str) -> None:
        if "ro" in options:
            os.chmod(self.image_path(image), 0o400)
        else:
            os.chmod(self.image_path(image), 0o600)

    def image_unmap(self, image: str) -> None:
        os.chmod(self.image_path(image), 0o000)

    def image_unmap_all(self) -> None:
        for entry in self.pool_dir.iterdir():
            if entry.is_file():
                entry.chmod(0o000)


def pool_from_settings(
    shards_settings: settings.Shards,
    shards_pool_settings: settings.ShardsPool,
) -> Pool:
    """Return a Pool from the settings"""
    pool_type = shards_pool_settings["type"]
    if pool_type == "rbd":
        rbd_settings = settings.rbd_shards_pool_settings_with_defaults(
            shards_pool_settings
        )
        return RBDPool(
            shard_max_size=shards_settings["max_size"],
            rbd_use_sudo=rbd_settings["use_sudo"],
            rbd_pool_name=rbd_settings["pool_name"],
            rbd_data_pool_name=rbd_settings["data_pool_name"],
            rbd_image_features_unsupported=rbd_settings["image_features_unsupported"],
            rbd_map_options=rbd_settings["map_options"],
        )
    elif pool_type == "directory":
        dir_settings = settings.directory_shards_pool_settings_with_defaults(
            shards_pool_settings
        )
        return FileBackedPool(
            shard_max_size=shards_settings["max_size"],
            base_directory=Path(dir_settings["base_directory"]),
            pool_name=dir_settings["pool_name"],
        )
    else:
        raise ValueError(f"Unknown shards pool type: {pool_type}")


def record_shard_mapped(base: SharedBase, shard_name: str):
    """Record a shard as mapped, bailing out after a few attempts.

    Multiple attempts are used to handle a race condition when two hosts
    attempt to record the shard as mapped at the same time. In this
    situation, one of the two hosts will succeed and the other one will
    fail, the sleep delay can be kept short and linear.

    """
    outer_exc = None
    for attempt in range(5):
        try:
            base.record_shard_mapped(host=socket.gethostname(), name=shard_name)
            break
        except Exception as exc:
            outer_exc = exc
            logger.warning("Failed to mark shard %s as mapped, retrying...", shard_name)
            time.sleep(attempt + 1)
    else:
        assert outer_exc is not None
        raise outer_exc


def manage_images(
    pool: Pool,
    base_dsn: str,
    manage_rw_images: bool,
    wait_for_image: Callable[[int], None],
    stop_running: Callable[[], bool],
    only_prefix: Optional[str] = None,
    application_name: Optional[str] = None,
) -> None:
    """Manage RBD image creation and mapping automatically.

    Arguments:
      base_dsn: the DSN of the connection to the SharedBase
      manage_rw_images: whether RW images should be created and mapped
      wait_for_image: function which is called at each loop iteration, with
        an attempt number, if no images had to be mapped recently
      stop_running: callback that returns True when the manager should stop running
      only_prefix: only map images with the given name prefix
      application_name: the application name sent to PostgreSQL
    """
    application_name = application_name or "Winery RBD image manager"
    base = SharedBase(base_dsn=base_dsn, application_name=application_name)

    mapped_images: Dict[str, Literal["ro", "rw"]] = {}

    attempt = 0
    notified_systemd = False
    while not stop_running():
        did_something = False
        logger.debug("Listing shards")
        start = time.monotonic()
        shards = [
            (shard_name, shard_state)
            for shard_name, shard_state in base.list_shards()
            if not only_prefix or shard_name.startswith(only_prefix)
        ]
        random.shuffle(shards)
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
                if shard_state == ShardState.PACKED:
                    record_shard_mapped(base, shard_name)
                continue
            elif shard_state.image_available:
                check_mapped = pool.image_mapped(shard_name)
                if check_mapped == "ro":
                    logger.debug(
                        "Detected %s shard %s, already mapped read-only",
                        shard_state.name,
                        shard_name,
                    )
                elif check_mapped == "rw":
                    logger.info(
                        "Detected %s shard %s, remapping read-only",
                        shard_state.name,
                        shard_name,
                    )
                    pool.image_remap_ro(shard_name)
                    attempt = 0
                    while pool.image_mapped(shard_name) != "ro":
                        attempt += 1
                        time.sleep(0.1)
                        if attempt % 100 == 0:
                            logger.warning(
                                "Waiting for %s shard %s to be remapped "
                                "read-only (for %ds)",
                                shard_state.name,
                                shard_name,
                                attempt / 10,
                            )
                    record_shard_mapped(base, shard_name)
                    did_something = True
                else:
                    logger.debug(
                        "Detected %s shard %s, mapping read-only",
                        shard_state.name,
                        shard_name,
                    )
                    pool.image_map(shard_name, options="ro")
                    record_shard_mapped(base, shard_name)
                    did_something = True
                mapped_images[shard_name] = "ro"
            elif manage_rw_images:
                if os.path.exists(pool.image_path(shard_name)):
                    # Image already mapped, nothing to do
                    pass
                elif not pool.image_exists(shard_name):
                    logger.info(
                        "Detected %s shard %s, creating RBD image",
                        shard_state.name,
                        shard_name,
                    )
                    pool.image_create(shard_name)
                    did_something = True
                else:
                    logger.warning(
                        "Detected %s shard %s and RBD image exists, mapping read-write",
                        shard_state.name,
                        shard_name,
                    )
                    pool.image_map(shard_name, "rw")
                    did_something = True
                # Now the shard is mapped
                mapped_images[shard_name] = "rw"
            else:
                logger.debug("%s shard %s, skipping", shard_state.name, shard_name)

            notify(
                "STATUS="
                f"Enumerated {len(shards)} shards, "
                f"mapped {len(mapped_images)} images"
            )

        if not notified_systemd:
            # The first iteration has happened, all known shards should be ready
            notify("READY=1")
            notified_systemd = True

        if did_something:
            attempt = 0
        else:
            # Sleep using the current value
            wait_for_image(attempt)
            attempt += 1


class ROShard:
    def __init__(self, name, throttler, pool):
        self.pool = pool
        image_status = self.pool.image_mapped(name)

        if image_status != "ro":
            raise ShardNotMapped(
                f"RBD image for {name} isn't mapped{' read-only' if image_status=='rw' else ''}"
            )

        self.throttler = throttler
        self.name = name
        self.path = self.pool.image_path(self.name)
        self.shard = None
        self.open()
        logger.debug("ROShard %s: loaded", self.name)

    def open(self):
        try:
            self.shard = Shard(self.path)
        except FileNotFoundError:
            raise ShardNotMapped(f"RBD image for {self.name} not found at {self.path}")

    def get(self, key):
        if not self.shard:
            self.open()

        return self.throttler.throttle_get(self.shard.lookup, key)

    def close(self):
        if shard := getattr(self, "shard", None):
            shard.close()
        self.shard = None

    def __del__(self):
        self.close()

    @staticmethod
    def delete(pool, shard_name, obj_id):
        image_status = pool.image_mapped(shard_name)
        if image_status == "ro":
            raise PermissionError(
                f"Cannot delete object from {shard_name}, mapped read-only"
            )
        if not image_status:
            pool.image_map(shard_name, options="rw")
        Shard.delete(pool.image_path(shard_name), obj_id)


class ROShardCreator:
    """Helper for Read-Only shard creation.

    Arguments:
      name: Name of the shard to be initialized
      count: Number of objects to provision in the shard
      throttler: An instance of a winery throttler
      rbd_create_images: whether the ROShardCreator should create the rbd
        image, or delegate to the rbd_shard_manager
      rbd_wait_for_image: function called when waiting for a shard to be mapped
      shard_max_size: the size of the shard, passed to :class:`Pool`
      rbd_*: other RBD-related :class:`Pool` arguments
    """

    def __init__(
        self,
        name: str,
        count: int,
        throttler: Throttler,
        pool: Pool,
        rbd_create_images: bool = True,
        rbd_wait_for_image: Callable[[int], None] = sleep_exponential(
            min_duration=5,
            factor=2,
            max_duration=60,
            message="Waiting for RBD image mapping",
        ),
        **kwargs,
    ):
        self.pool = pool
        self.throttler = throttler
        self.name = name
        self.count = count
        self.path = self.pool.image_path(self.name)
        self.rbd_create_images = rbd_create_images
        self.rbd_wait_for_image = rbd_wait_for_image

    def __enter__(self) -> "ROShardCreator":
        if self.rbd_create_images:
            self.pool.image_create(self.name)
        else:
            attempt = 0
            while not os.path.exists(self.path):
                self.rbd_wait_for_image(attempt)
                attempt += 1

        self.zero_image_if_needed()

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

    def zero_image_if_needed(self):
        """Check whether the image is empty, and zero it out if it's not.

        We really check only the first 1kB, as we assume that the SWHShard
        marker will have been written at the beginning of the image under all
        circumstances if the RO Shard creation has been interrupted.
        """
        with open(self.path, "rb") as f:
            start = f.read(1024)
            if set(start) == {0}:
                return

        logger.warning("RO Shard %s isn't empty, cleaning it up", self.path)
        st = os.stat(self.path)
        if stat.S_ISBLK(st.st_mode):
            # Block device, use DISCARD
            command = ["/usr/sbin/blkdiscard", self.path]
        else:
            # Regular file, use fallocate --punch-hole
            command = [
                "/usr/bin/fallocate",
                "--punch-hole",
                "-l",
                str(st.st_size),
                self.path,
            ]
        try:
            subprocess.run(command, check=True, capture_output=True)
        except subprocess.CalledProcessError:
            logger.warning("%s failed:", shlex.join(command), self.path, exc_info=True)

    def add(self, content, obj_id):
        return self.throttler.throttle_add(self.shard.write, obj_id, content)
