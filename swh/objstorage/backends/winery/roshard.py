# Copyright (C) 2021-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter
import logging
import os
import random
import shlex
import socket
import stat
import subprocess
import time
from types import TracebackType
from typing import Callable, Dict, Literal, Optional, Type

from systemd.daemon import notify

from swh.shard import Shard, ShardCreator

from .pools import Pool
from .sharedbase import ShardState, SharedBase
from .sleep import sleep_exponential

logger = logging.getLogger(__name__)


class ShardNotMapped(Exception):
    pass


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
    def __init__(self, name, pool):
        self.pool = pool
        image_status = self.pool.image_mapped(name)

        if image_status != "ro":
            raise ShardNotMapped(
                f"RBD image for {name} isn't mapped"
                f"{' read-only' if image_status == 'rw' else ''}"
            )

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

        return self.shard.lookup(key)

    def close(self):
        if shard := getattr(self, "shard", None):
            shard.close()
        self.shard = None

    def __del__(self):
        self.close()

    @staticmethod
    def delete(pool: Pool, shard_name: str, obj_id: bytes):
        image_status = pool.image_mapped(shard_name)
        if image_status == "ro":
            raise PermissionError(
                f"Cannot delete object from {shard_name}, mapped read-only"
            )
        if not image_status:
            pool.image_map(shard_name, options="rw")
        pool.delete_object(shard_name, obj_id)


class ROShardCreator:
    """Helper for Read-Only shard creation.

    Arguments:
      name: Name of the shard to be initialized
      count: Number of objects to provision in the shard
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
        if exc_type is None:
            self.shard.__exit__(exc_type, exc_val, exc_tb)
            if self.rbd_create_images:
                self.pool.image_remap_ro(self.name)

    def zero_image_if_needed(self):
        """Check whether the image is empty, and zero it out if it's not.

        We really check only the first 1kB, as we assume that the SWHShard
        marker will have been written at the beginning of the image under all
        circumstances if the RO Shard creation has been interrupted.
        """
        with open(self.path, "rb") as f:
            start = f.read(1024)
            if not start or set(start) == {0}:
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
        return self.shard.write(obj_id, content)
