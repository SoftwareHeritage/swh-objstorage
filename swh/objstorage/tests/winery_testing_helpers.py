# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from typing import Iterable

from swh.objstorage.backends.winery.roshard import Pool

logger = logging.getLogger(__name__)


class SharedBaseHelper:
    def __init__(self, sharedbase):
        self.sharedbase = sharedbase

    def get_shard_info_by_name(self, name):
        with self.sharedbase.db.cursor() as c:
            c.execute("SELECT readonly, packing FROM shards WHERE name = %s", (name,))
            if c.rowcount == 0:
                return None
            else:
                return c.fetchone()


class PoolHelper(Pool):
    def ceph(self, *arguments) -> Iterable[str]:
        """Run sudo ceph with the given arguments"""

        cli = ["sudo", "ceph", *arguments]

        return self.run(*cli)

    def image_delete(self, image):
        self.image_unmap(image)
        self.rbd("remove", image)

    def images_clobber(self):
        for image in self.image_list():
            self.image_unmap(image)

    def clobber(self):
        self.images_clobber()
        self.pool_clobber()

    def pool_clobber(self):
        for pool in (self.name, f"{self.name}-data"):
            self.ceph(
                "osd",
                "pool",
                "delete",
                pool,
                pool,
                "--yes-i-really-really-mean-it",
            )

    def pool_create(self):
        data = f"{self.name}-data"
        self.ceph(
            "osd",
            "erasure-code-profile",
            "set",
            "--force",
            data,
            "k=4",
            "m=2",
            "crush-failure-domain=host",
        )
        self.ceph("osd", "pool", "create", data, "100", "erasure", data)
        self.ceph("osd", "pool", "set", data, "allow_ec_overwrites", "true")
        self.ceph("osd", "pool", "set", data, "pg_autoscale_mode", "off")
        self.ceph("osd", "pool", "create", self.name)
