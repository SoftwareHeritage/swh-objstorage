# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

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
    def image_delete(self, image):
        self.image_unmap(image)
        logger.info(f"rdb --pool {self.name} remove {image}")
        self.rbd.remove(image)

    def images_clobber(self):
        for image in self.image_list():
            image = image.strip()
            self.image_unmap(image)

    def clobber(self):
        self.images_clobber()
        self.pool_clobber()

    def pool_clobber(self):
        logger.info(f"ceph osd pool delete {self.name}")
        self.ceph.osd.pool.delete(self.name, self.name, "--yes-i-really-really-mean-it")
        data = f"{self.name}-data"
        logger.info(f"ceph osd pool delete {data}")
        self.ceph.osd.pool.delete(data, data, "--yes-i-really-really-mean-it")

    def pool_create(self):
        data = f"{self.name}-data"
        logger.info(f"ceph osd pool create {data}")
        self.ceph.osd(
            "erasure-code-profile",
            "set",
            "--force",
            data,
            "k=4",
            "m=2",
            "crush-failure-domain=host",
        )
        self.ceph.osd.pool.create(data, "100", "erasure", data)
        self.ceph.osd.pool.set(data, "allow_ec_overwrites", "true")
        self.ceph.osd.pool.set(data, "pg_autoscale_mode", "off")
        logger.info(f"ceph osd pool create {self.name}")
        self.ceph.osd.pool.create(self.name)
