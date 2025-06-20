# Copyright (C) 2021-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import atexit
import logging
from subprocess import CalledProcessError
from typing import Iterable, Optional, Tuple

from swh.objstorage.backends.winery.roshard import RBDPool
from swh.objstorage.backends.winery.settings import DEFAULT_IMAGE_FEATURES_UNSUPPORTED

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


class RBDPoolHelper(RBDPool):
    def __init__(
        self,
        shard_max_size: int,
        rbd_use_sudo: bool = True,
        rbd_pool_name: str = "test-shards",
        rbd_data_pool_name: Optional[str] = None,
        rbd_image_features_unsupported: Tuple[
            str, ...
        ] = DEFAULT_IMAGE_FEATURES_UNSUPPORTED,
        rbd_map_options: str = "",
        rbd_erasure_code_profile=None,
        rbd_data_pool_settings=None,
    ):
        super().__init__(
            shard_max_size=shard_max_size,
            rbd_use_sudo=rbd_use_sudo,
            rbd_pool_name=rbd_pool_name,
            rbd_data_pool_name=rbd_data_pool_name,
            rbd_image_features_unsupported=rbd_image_features_unsupported,
            rbd_map_options=rbd_map_options,
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

    POOL_CONFIG = RBDPool.POOL_CONFIG + (
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
