# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

import sh

from swh.perfecthash import Shard

from .throttler import Throttler

logger = logging.getLogger(__name__)


class Pool(object):
    name = "shards"

    def __init__(self, **kwargs):
        self.args = kwargs
        self.rbd = sh.sudo.bake("rbd", f"--pool={self.name}")
        self.ceph = sh.sudo.bake("ceph")
        self.image_size = int((self.args["shard_max_size"] * 2) / (1024 * 1024))

    def image_list(self):
        try:
            self.rbd.ls()
        except sh.ErrorReturnCode_2 as e:
            if "No such file or directory" in e.args[0]:
                return []
            else:
                raise
        return [image.strip() for image in self.rbd.ls()]

    def image_path(self, image):
        return f"/dev/rbd/{self.name}/{image}"

    def image_create(self, image):
        logger.info(f"rdb --pool {self.name} create --size={self.image_size} {image}")
        self.rbd.create(
            f"--size={self.image_size}", f"--data-pool={self.name}-data", image
        )
        self.rbd.feature.disable(
            f"{self.name}/{image}", "object-map", "fast-diff", "deep-flatten"
        )
        self.image_map(image, "rw")

    def image_map(self, image, options):
        self.rbd.device("map", "-o", options, image)
        sh.sudo("chmod", "777", self.image_path(image))

    def image_remap_ro(self, image):
        self.image_unmap(image)
        self.image_map(image, "ro")

    def image_unmap(self, image):
        self.rbd.device.unmap(f"{self.name}/{image}", _ok_code=(0, 22))


class ROShard:
    def __init__(self, name, **kwargs):
        self.pool = Pool(shard_max_size=kwargs["shard_max_size"])
        self.throttler = Throttler(**kwargs)
        self.name = name

    def create(self, count):
        self.pool.image_create(self.name)
        self.shard = Shard(self.pool.image_path(self.name))
        return self.shard.create(count)

    def load(self):
        self.shard = Shard(self.pool.image_path(self.name))
        return self.shard.load() == self.shard

    def get(self, key):
        return self.throttler.throttle_get(self.shard.lookup, key)

    def add(self, content, obj_id):
        return self.throttler.throttle_add(self.shard.write, obj_id, content)

    def save(self):
        return self.shard.save()
