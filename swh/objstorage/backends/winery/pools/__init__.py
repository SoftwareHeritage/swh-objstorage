# Copyright (C) 2021-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
from pathlib import Path
import shlex
import stat
import subprocess
from types import TracebackType
from typing import List, Literal, Optional, Protocol

from .. import settings

logger = logging.getLogger(__name__)


class ImageReader(Protocol):
    """
    Protocol for images' reader classes. This allows a pool to pick a class depending
    on the backing file format (currently swh.shard.Shard or swh.mosaic.MosaicReader)
    """

    def lookup(self, key: bytes) -> bytes | None: ...

    def close(self) -> None: ...


class ImageWriter(Protocol):
    """
    Protocol for images' writer classes. This allows a pool to pick a class depending
    on the backing file format (currently swh.shard.ShardCreator or
    swh.mosaic.MosaicWriter).
    """

    def __enter__(self) -> "ImageWriter": ...

    def __exit__(
        self,
        exc_type: Optional[BaseException],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool: ...

    def write(self, key: bytes, object: bytes) -> None: ...


class Pool(Protocol):
    pool_name: str

    def image_exists(self, image: str) -> bool:
        """Check whether the named image exists (it does not have to be mapped)"""
        ...

    def image_mapped(self, image: str) -> Optional[Literal["ro", "rw"]]:
        """Check whether the image is already mapped, read-only or read-write"""
        imgpath = self.image_path(image)
        if os.access(imgpath, os.R_OK):
            if os.access(imgpath, os.W_OK):
                return "rw"
            return "ro"
        return None

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

    def image_import(self, image: str) -> None:
        """Import an existing image file in the current pool"""
        ...

    def image_open(self, image: str) -> ImageReader: ...

    def delete_object(self, shard_name: str, obj_id: bytes) -> None: ...

    def open_writer(
        self, shard_name: str, nb_objects: int, xcreate_image: bool
    ) -> ImageWriter:
        """Instantiate the correct ImageWriter object for the given shard

        This is used at time of packing a batch of objects in a shard file.
        """
        ...

    @staticmethod
    def _zero_image_if_needed(path):
        """Check whether the image is empty, and zero it out if it's not.

        We really check only the first 1kB, as we assume that the SWHShard
        marker will have been written at the beginning of the image under all
        circumstances if the RO Shard creation has been interrupted.
        """
        with open(path, "rb") as f:
            start = f.read(1024)
            if not start or set(start) == {0}:
                return

        logger.warning("RO image %s isn't empty, cleaning it up", path)
        st = os.stat(path)
        if stat.S_ISBLK(st.st_mode):
            # Block device, use DISCARD
            command = ["/usr/sbin/blkdiscard", path]
        else:
            # Regular file, use fallocate --punch-hole
            command = [
                "/usr/bin/fallocate",
                "--punch-hole",
                "-l",
                str(st.st_size),
                path,
            ]
        try:
            subprocess.run(command, check=True, capture_output=True)
        except subprocess.CalledProcessError:
            logger.warning("%s failed:", shlex.join(command), path, exc_info=True)


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

    def image_import(self, image: str) -> None:
        name = os.path.basename(image)
        dst = self.image_path(name)
        os.link(image, dst)


def pool_from_settings(
    shards_settings: settings.Shards,
    shards_pool_settings: settings.ShardsPool,
) -> Pool:
    """Return a Pool from the settings"""
    pool_type = shards_pool_settings["type"]
    if pool_type == "rbd":
        from .rbd import RBDPool

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
        from .shard import ShardBackedPool

        dir_settings = settings.directory_shards_pool_settings_with_defaults(
            shards_pool_settings
        )
        return ShardBackedPool(
            shard_max_size=shards_settings["max_size"],
            base_directory=Path(dir_settings["base_directory"]),
            pool_name=dir_settings["pool_name"],
        )
    else:
        raise ValueError(f"Unknown shards pool type: {pool_type}")
