# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import math
import os.path
import subprocess
from types import TracebackType
from typing import Iterable, Optional, Type

from swh.perfecthash import Shard, ShardCreator

from .throttler import Throttler

logger = logging.getLogger(__name__)


class Pool(object):
    name = "shards"

    def __init__(self, **kwargs):
        self.args = kwargs
        self.image_size = math.ceil((self.args["shard_max_size"] * 2) / (1024 * 1024))

    def run(self, *cmd: str) -> Iterable[str]:
        """Run the given command, and return its output as lines.

        Return: the standard output of the run command

        Raises: CalledProcessError if the command doesn't exit with exit code 0.
        """

        logger.info(" ".join(repr(item) if " " in item else item for item in cmd))
        result = subprocess.check_output(cmd, encoding="utf-8", stderr=subprocess.PIPE)

        return result.splitlines()

    def rbd(self, *arguments: str) -> Iterable[str]:
        """Run sudo rbd with the given arguments"""

        cli = ["sudo", "rbd", f"--pool={self.name}", *arguments]

        return self.run(*cli)

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
        return f"/dev/rbd/{self.name}/{image}"

    def image_create(self, image):
        self.rbd(
            "create",
            f"--size={self.image_size}",
            f"--data-pool={self.name}-data",
            image,
        )
        self.rbd(
            "feature",
            "disable",
            f"{self.name}/{image}",
            "object-map",
            "fast-diff",
            "deep-flatten",
        )
        self.image_map(image, "rw")

    def image_map(self, image, options):
        self.rbd("device", "map", "-o", options, image)
        self.run("sudo", "chmod", "777", self.image_path(image))

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


class ROShard:
    def __init__(self, name, **kwargs):
        self.pool = Pool(shard_max_size=kwargs["shard_max_size"])
        self.throttler = Throttler(**kwargs)
        self.name = name
        self.path = self.pool.image_path(self.name)
        self.shard = Shard(self.path)
        logger.debug("ROShard %s: loaded", self.name)

    def get(self, key):
        return self.throttler.throttle_get(self.shard.lookup, key)


class ROShardCreator:
    def __init__(self, name: str, count: int, **kwargs):
        self.pool = Pool(shard_max_size=kwargs["shard_max_size"])
        self.throttler = Throttler(**kwargs)
        self.name = name
        self.count = count
        self.path = self.pool.image_path(self.name)

    def __enter__(self) -> "ROShardCreator":
        self.pool.image_create(self.name)
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
        if not exc_type:
            self.pool.image_remap_ro(self.name)

    def add(self, content, obj_id):
        return self.throttler.throttle_add(self.shard.write, obj_id, content)
