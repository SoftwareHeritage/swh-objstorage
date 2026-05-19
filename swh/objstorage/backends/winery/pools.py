# Copyright (C) 2021-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import math
import os
from pathlib import Path
import subprocess
from typing import Iterable, List, Literal, Optional, Protocol, Tuple

from . import settings

logger = logging.getLogger(__name__)


class Pool(Protocol):
    pool_name: str

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
            if os.stat(path).st_mode == 0o100600:
                # If the image exists but is -rw------- it is expected to be a
                # dandling/stale shard file left by a crashed/aborted packing
                # process
                logger.warning("Stale image found. Reusing it")
            else:
                raise ValueError(f"Image {image} already exists")
        open(path, "w").close()
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
