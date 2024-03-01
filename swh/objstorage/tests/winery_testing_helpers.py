# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import atexit
import logging
import os
from pathlib import Path
from subprocess import CalledProcessError
from typing import Iterable, List, Optional, Tuple

from swh.objstorage.backends.winery.roshard import (
    DEFAULT_IMAGE_FEATURES_UNSUPPORTED,
    Pool,
)

logger = logging.getLogger(__name__)


DEFAULT_ERASURE_CODE_PROFILE = {
    "name": "winery-test-profile",
    "k": "4",
    "m": "2",
    "crush-failure-domain": "host",
}
DEFAULT_DATA_POOL_SETTINGS = {
    "allow_ec_overwrites": "true",
    "pg_autoscale_mode": "off",
    "pg_num": 128,
}


class PoolHelper(Pool):
    def __init__(
        self,
        shard_max_size: int,
        rbd_use_sudo: bool = True,
        rbd_pool_name: str = "test-shards",
        rbd_data_pool_name: Optional[str] = None,
        rbd_image_features_unsupported: Tuple[
            str, ...
        ] = DEFAULT_IMAGE_FEATURES_UNSUPPORTED,
        rbd_erasure_code_profile=None,
        rbd_data_pool_settings=None,
    ):
        super().__init__(
            shard_max_size=shard_max_size,
            rbd_use_sudo=rbd_use_sudo,
            rbd_pool_name=rbd_pool_name,
            rbd_data_pool_name=rbd_data_pool_name,
            rbd_image_features_unsupported=rbd_image_features_unsupported,
        )
        self.erasure_code_profile_settings = (
            rbd_erasure_code_profile or DEFAULT_ERASURE_CODE_PROFILE.copy()
        )
        self.erasure_code_profile = self.erasure_code_profile_settings.pop(
            "name", DEFAULT_ERASURE_CODE_PROFILE["name"]
        )

        self.data_pool_settings = (
            rbd_data_pool_settings or DEFAULT_DATA_POOL_SETTINGS.copy()
        )
        self.data_pool_pg_num = self.data_pool_settings.pop(
            "pg_num", DEFAULT_DATA_POOL_SETTINGS["pg_num"]
        )

    POOL_CONFIG = Pool.POOL_CONFIG + (
        "rbd_erasure_code_profile",
        "rbd_data_pool_settings",
    )

    def ceph(self, *arguments) -> Iterable[str]:
        """Run ceph with the given arguments"""

        return self.run("ceph", *arguments)

    def image_remove(self, image):
        self.image_unmap(image)
        self.rbd("remove", image)

    def images_remove(self):
        for image in self.image_list():
            try:
                self.image_remove(image)
            except CalledProcessError:
                logger.error(
                    "Could not remove image %s, we'll try again in an atexit handler...",
                    image,
                )
                atexit.register(self.image_remove, image)
                pass

    def remove(self):
        self.images_remove()
        self.pool_remove()

    def pool_remove(self):
        for pool in (self.pool_name, self.data_pool_name):
            self.ceph(
                "osd",
                "pool",
                "delete",
                pool,
                pool,
                "--yes-i-really-really-mean-it",
            )

    def pool_create(self):
        try:
            output = self.ceph(
                "osd",
                "erasure-code-profile",
                "get",
                self.erasure_code_profile,
            )
        except CalledProcessError:
            self.ceph(
                "osd",
                "erasure-code-profile",
                "set",
                self.erasure_code_profile,
                *(f"{k}={v}" for k, v in self.erasure_code_profile_settings.items()),
            )
        else:
            current_settings = dict(line.split("=", 1) for line in output)
            for k, v in self.erasure_code_profile_settings.items():
                if (current_setting := current_settings[k]) != str(v):
                    logger.warning(
                        "For erasure coding profile %s, setting %s=%s != requested %s",
                        self.erasure_code_profile,
                        k,
                        current_setting,
                        v,
                    )

        self.ceph(
            "osd",
            "pool",
            "create",
            self.data_pool_name,
            f"{self.data_pool_pg_num}",
            "erasure",
            self.erasure_code_profile,
        )

        for setting, value in self.data_pool_settings.items():
            self.ceph("osd", "pool", "set", self.data_pool_name, setting, value)

        self.ceph("osd", "pool", "create", self.pool_name)


class FileBackedPool(Pool):
    """File-backed pool for Winery shards mimicking a Ceph RBD pool.

    Unmapped images are represented by setting the file permission to 0o000.
    """

    base_directory: Optional[Path] = None

    def __init__(
        self,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        assert (
            FileBackedPool.base_directory is not None
        ), "set_base_directory() should have been called first"
        self.pool_dir = FileBackedPool.base_directory / self.pool_name
        self.pool_dir.mkdir(exist_ok=True)

    @classmethod
    def set_base_directory(cls, base_directory: Path) -> None:
        cls.base_directory = base_directory

    @classmethod
    def from_kwargs(cls, **kwargs) -> "Pool":
        """Create a Pool from a set of arbitrary keyword arguments"""
        return cls(**{k: kwargs[k] for k in Pool.POOL_CONFIG if k in kwargs})

    def run(self, *cmd: str) -> Iterable[str]:
        raise NotImplementedError

    def rbd(self, *arguments: str) -> Iterable[str]:
        raise NotImplementedError

    def image_exists(self, image: str) -> bool:
        return (self.pool_dir / image).is_file()

    def image_list(self) -> List[str]:
        return [entry.name for entry in self.pool_dir.iterdir() if entry.is_file()]

    def image_path(self, image: str) -> str:
        return str(self.pool_dir / image)

    def image_create(self, image: str) -> None:
        path = self.image_path(image)
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
