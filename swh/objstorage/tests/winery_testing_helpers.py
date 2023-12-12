# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import atexit
import logging
from subprocess import CalledProcessError
from typing import Iterable, Optional, Tuple

from swh.objstorage.backends.winery.roshard import (
    DEFAULT_IMAGE_FEATURES_UNSUPPORTED,
    Pool,
)
from swh.objstorage.backends.winery.sharedbase import ShardState

logger = logging.getLogger(__name__)


class SharedBaseHelper:
    def __init__(self, sharedbase):
        self.sharedbase = sharedbase

    def get_shard_info_by_name(self, name):
        with self.sharedbase.db.cursor() as c:
            c.execute("SELECT state FROM shards WHERE name = %s", (name,))
            row = c.fetchone()
            if not row:
                return None
            return ShardState(row[0])


DEFAULT_ERASURE_CODE_PROFILE = {
    "name": "winery",
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
        """Run sudo ceph with the given arguments"""

        cli = ["sudo", "ceph", *arguments]

        return self.run(*cli)

    def image_delete(self, image):
        self.image_unmap(image)
        self.rbd("remove", image)

    def images_clobber(self):
        for image in self.image_list():
            try:
                self.image_unmap(image)
            except CalledProcessError:
                logger.error(
                    "Could not unmap image %s, we'll try again in an atexit handler...",
                    image,
                )
                atexit.register(self.image_unmap, image)
                pass

    def clobber(self):
        self.images_clobber()
        self.pool_clobber()

    def pool_clobber(self):
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
        self.ceph(
            "osd",
            "erasure-code-profile",
            "set",
            "--force",
            self.erasure_code_profile,
            *(f"{k}={v}" for k, v in self.erasure_code_profile_settings.items()),
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
