# Copyright (C) 2022-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from multiprocessing import Process
from typing import Callable, Optional, Tuple

from typing_extensions import Literal

from swh.objstorage import exc
from swh.objstorage.interface import ObjId
from swh.objstorage.objstorage import ObjStorage

from .roshard import (
    DEFAULT_IMAGE_FEATURES_UNSUPPORTED,
    Pool,
    ROShard,
    ROShardCreator,
    ShardNotMapped,
)
from .rwshard import RWShard
from .sharedbase import ShardState, SharedBase
from .sleep import sleep_exponential
from .stats import Stats

logger = logging.getLogger(__name__)


class WineryObjStorage(ObjStorage):
    PRIMARY_HASH: Literal["sha256"] = "sha256"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if kwargs.get("readonly"):
            self.winery = WineryReader(**kwargs)
        else:
            self.winery = WineryWriter(**kwargs)

    def uninit(self):
        self.winery.uninit()

    def get(self, obj_id: ObjId) -> bytes:
        return self.winery.get(self._hash(obj_id))

    def check_config(self, *, check_write: bool) -> bool:
        return True

    def __contains__(self, obj_id: ObjId) -> bool:
        return self._hash(obj_id) in self.winery

    def add(self, content: bytes, obj_id: ObjId, check_presence: bool = True) -> None:
        self.winery.add(content, self._hash(obj_id), check_presence)

    def check(self, obj_id: ObjId) -> None:
        return self.winery.check(self._hash(obj_id))

    def delete(self, obj_id: ObjId):
        if not self.allow_delete:
            raise PermissionError("Delete is not allowed.")
        return self.winery.delete(obj_id)

    def _hash(self, obj_id: ObjId) -> bytes:
        if isinstance(obj_id, dict):
            return obj_id[self.PRIMARY_HASH]
        else:
            return obj_id


class WineryBase:
    def __init__(self, **kwargs):
        self.args = kwargs
        self.init()

    def init(self):
        self.base = SharedBase(**self.args)

    def uninit(self):
        self.base.uninit()

    def __contains__(self, obj_id):
        return self.base.contains(obj_id)


class WineryReader(WineryBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ro_shards = {}
        self.rw_shards = {}

    def roshard(self, name) -> Optional[ROShard]:
        if name not in self.ro_shards:
            try:
                shard = ROShard(name, **self.args)
            except ShardNotMapped:
                return None
            self.ro_shards[name] = shard
            if name in self.rw_shards:
                self.rw_shards[name].uninit()
                del self.rw_shards[name]
        return self.ro_shards[name]

    def rwshard(self, name) -> RWShard:
        if name not in self.rw_shards:
            shard = RWShard(name, **self.args)
            self.rw_shards[name] = shard
        return self.rw_shards[name]

    def get(self, obj_id: ObjId) -> bytes:
        shard_info = self.base.get(obj_id)
        if shard_info is None:
            raise exc.ObjNotFoundError(obj_id)
        name, state = shard_info
        content: Optional[bytes] = None
        if state.image_available:
            roshard = self.roshard(name)
            if roshard:
                content = roshard.get(obj_id)
        if content is None:
            rwshard = self.rwshard(name)
            content = rwshard.get(obj_id)
        if content is None:
            raise exc.ObjNotFoundError(obj_id)
        return content

    def uninit(self):
        for shard in self.rw_shards.values():
            shard.uninit()
        self.rw_shards = {}
        for shard in self.ro_shards.values():
            shard.close()
        self.ro_shards = {}
        super().uninit()


def pack(shard, shared_base=None, clean_immediately=False, **kwargs):
    stats = Stats(kwargs.get("output_dir"))
    rw = RWShard(shard, **kwargs)

    count = rw.count()
    logger.info("Creating RO shard %s for %s objects", shard, count)
    with ROShardCreator(shard, count, **kwargs) as ro:
        logger.info("Created RO shard %s", shard)
        for i, (obj_id, content) in enumerate(rw.all()):
            ro.add(content, obj_id)
            if stats.stats_active:
                stats.stats_read(obj_id, content)
                stats.stats_write(obj_id, content)
            if i % 100 == 99:
                logger.debug("RO shard %s: added %s/%s objects", shard, i + 1, count)

        logger.debug("RO shard %s: added %s objects, saving", shard, count)

    logger.debug("RO shard %s: saved", shard)

    uninit_base = False
    if not shared_base:
        shared_base = SharedBase(**kwargs)
        uninit_base = True
    shared_base.shard_packing_ends(shard)
    rw.uninit()
    if clean_immediately:
        cleanup_rw_shard(shard, shared_base=shared_base, **kwargs)
    if uninit_base:
        shared_base.uninit()
    return True


def cleanup_rw_shard(shard, shared_base=None, **kwargs):
    rw = RWShard(shard, **{"shard_max_size": 0, **kwargs})

    uninit_base = False
    try:
        if not shared_base:
            shared_base = SharedBase(**kwargs)
            uninit_base = True

        rw.drop()
        shared_base.set_shard_state(name=shard, new_state=ShardState.READONLY)
    finally:
        if shared_base and uninit_base:
            shared_base.uninit()

    return True


class WineryWriter(WineryReader):
    def __init__(self, **kwargs):
        self.pack_immediately = kwargs.get("pack_immediately", True)
        self.clean_immediately = kwargs.get("clean_immediately", True)
        super().__init__(**kwargs)
        self.shards_filled = []
        self.packers = []

    def init(self):
        super().init()
        self.shard = RWShard(self.base.locked_shard, **self.args)
        logger.debug("WineryBase: RWShard %s instantiated", self.base.locked_shard)

    def uninit(self):
        self.shard.uninit()
        super().uninit()

    def add(self, content: bytes, obj_id: ObjId, check_presence: bool = True) -> None:
        if check_presence and obj_id in self:
            return

        shard = self.base.add_phase_1(obj_id)
        if shard != self.base.locked_shard_id:
            #  this object is the responsibility of another shard
            return

        self.shard.add(obj_id, content)
        self.base.add_phase_2(obj_id)

        if self.shard.is_full():
            self.base.set_shard_state(new_state=ShardState.FULL)
            self.shards_filled.append(self.shard.name)
            if self.pack_immediately:
                self.pack()
            else:
                # Switch shards
                self.uninit()
                self.init()

    def delete(self, obj_id: ObjId):
        shard_info = self.base.get(obj_id)
        if shard_info is None:
            raise exc.ObjNotFoundError(obj_id)
        name, state = shard_info
        # We only care about RWShard for now. ROShards will be
        # taken care in a batch job.
        if not state.image_available:
            rwshard = self.rwshard(name)
            try:
                rwshard.delete(obj_id)
            except KeyError:
                logger.warning(
                    "Shard %s does not seem to know about object %s, but we "
                    "had an entry in SharedBase (which is going to "
                    "be removed just now)",
                    rwshard.name,
                    obj_id,
                )
        self.base.delete(obj_id)

    def check(self, obj_id: ObjId) -> None:
        # load all shards packing == True and not locked (i.e. packer
        # was interrupted for whatever reason) run pack for each of them
        pass

    def pack(self):
        self.base.shard_packing_starts(self.shard.name)
        p = Process(
            target=pack,
            kwargs={
                "shard": self.shard.name,
                "clean_immediately": self.clean_immediately,
                **self.args,
            },
        )
        self.uninit()
        p.start()
        self.packers.append(p)
        self.init()

    def __del__(self):
        for p in self.packers:
            p.kill()
            p.join()


def never_stop(_: int) -> bool:
    return False


def stop_after_shards(max_shards_packed: int) -> Callable[[int], bool]:
    def stop(shards_packed: int):
        return shards_packed >= max_shards_packed

    return stop


def shard_packer(
    base_dsn: str,
    shard_dsn: str,
    shard_max_size: int,
    throttle_read: int,
    throttle_write: int,
    application_name: Optional[str] = None,
    rbd_pool_name: str = "shards",
    rbd_data_pool_name: Optional[str] = None,
    rbd_image_features_unsupported: Tuple[
        str, ...
    ] = DEFAULT_IMAGE_FEATURES_UNSUPPORTED,
    rbd_use_sudo: bool = True,
    rbd_create_images: bool = True,
    rbd_wait_for_image: Callable[[int], None] = sleep_exponential(
        min_duration=5,
        factor=2,
        max_duration=60,
        message="Waiting for RBD image mapping",
    ),
    output_dir: Optional[str] = None,
    stop_packing: Callable[[int], bool] = never_stop,
    wait_for_shard: Callable[[int], None] = sleep_exponential(
        min_duration=5,
        factor=2,
        max_duration=60,
        message="No shards to pack",
    ),
) -> int:
    """Pack shards until the `stop_packing` function returns True.

    When no shards are available for packing, call the `wait_for_shard` function.

    Arguments:
      base_dsn: PostgreSQL dsn for the shared database
      shard_dsn: PostgreSQL dsn for the individual shard databases
      shard_max_size: Max size of a shard (used to size new shards)
      throttle_read: reads per second
      throttle_write: writes per second
      application_name: the application name sent to PostgreSQL
      rbd_create_images: create images directly (or wait for RBD mapper)
      rbd_wait_for_image: sleep function called to wait for an image (when
       `rbd_create_images`=`False`)
      rbd_*: passed directly to :class:`roshard.Pool`
      output_dir: output directory for statistics
      stop_packing: callback to determine whether the packer should exit
      wait_for_shard: sleep function called when no shards are available to be packed
    """
    application_name = application_name or "Winery Shard Packer"

    base = SharedBase(base_dsn=base_dsn, application_name=application_name)

    shards_packed = 0
    waited_for_shards = 0
    while not stop_packing(shards_packed):
        shard_to_pack = base.lock_one_shard(
            current_state=ShardState.FULL, new_state=ShardState.PACKING
        )

        if not shard_to_pack:
            wait_for_shard(waited_for_shards)
            waited_for_shards += 1
            continue

        waited_for_shards = 0

        name, _ = shard_to_pack
        logger.info("shard_packer: Locked shard %s to pack", name)
        ret = pack(
            name,
            base_dsn=base_dsn,
            shard_dsn=shard_dsn,
            shard_max_size=shard_max_size,
            output_dir=output_dir,
            shared_base=base,
            throttle_read=throttle_read,
            throttle_write=throttle_write,
            application_name=application_name,
            rbd_use_sudo=rbd_use_sudo,
            rbd_create_images=rbd_create_images,
            rbd_wait_for_image=rbd_wait_for_image,
            rbd_pool_name=rbd_pool_name,
            rbd_data_pool_name=rbd_data_pool_name,
            rbd_image_features_unsupported=rbd_image_features_unsupported,
        )
        if not ret:
            raise ValueError("Packing shard %s failed" % name)
        shards_packed += 1

    return shards_packed


def rw_shard_cleaner(
    base_dsn: str,
    shard_dsn: str,
    min_mapped_hosts: int,
    application_name: Optional[str] = None,
    stop_cleaning: Callable[[int], bool] = never_stop,
    wait_for_shard: Callable[[int], None] = sleep_exponential(
        min_duration=5,
        factor=2,
        max_duration=60,
        message="No shards to clean up",
    ),
) -> int:
    """Clean up RW shards until the `stop_cleaning` function returns True.

    When no shards are available for packing, call the `wait_for_shard` function.

    Arguments:
      base_dsn: PostgreSQL dsn for the shared database
      shard_dsn: PostgreSQL dsn for the individual shard databases
      min_mapped_hosts: how many hosts should have mapped the image read-only before
        cleaning it
      application_name: the application name sent to PostgreSQL
      stop_cleaning: callback to determine whether the cleaner should exit
      wait_for_shard: sleep function called when no shards are available to be cleaned
    """
    application_name = application_name or "Winery RW shard cleaner"
    base = SharedBase(base_dsn=base_dsn, application_name=application_name)

    shards_cleaned = 0
    waited_for_shards = 0
    while not stop_cleaning(shards_cleaned):
        shard_to_clean = base.lock_one_shard(
            current_state=ShardState.PACKED,
            new_state=ShardState.CLEANING,
            min_mapped_hosts=min_mapped_hosts,
        )

        if not shard_to_clean:
            wait_for_shard(waited_for_shards)
            waited_for_shards += 1
            continue

        waited_for_shards = 0

        name, _ = shard_to_clean
        logger.info("rw_shard_cleaner: Locked shard %s to clean", name)

        ret = cleanup_rw_shard(
            name,
            base_dsn=base_dsn,
            shard_dsn=shard_dsn,
            shared_base=base,
            application_name=application_name,
        )
        if not ret:
            raise ValueError("Cleaning shard %s failed" % name)
        shards_cleaned += 1

    return shards_cleaned


def deleted_objects_cleaner(
    base: SharedBase,
    pool: Pool,
    stop_running: Callable[[], bool],
):
    """Clean up deleted objects from RO shards and the shared database.

    This requires the ability to map RBD images in read-write mode. Images will be
    left mapped by this process as it is meant to be executed in a transient host
    dedicated to this purpose.

    Arguments:
      base_dsn: PostgreSQL dsn for the shared database
      pool: Ceph RBD pool for Winery shards
      stop_running: callback that returns True when the manager should stop running
    """
    count = 0
    with base.db.cursor() as cur:
        for obj_id, shard_name, shard_state in base.deleted_objects(cur):
            if stop_running():
                break
            if shard_state.readonly:
                ROShard.delete(pool, shard_name, obj_id)
            base.clean_delete_object(cur, obj_id)
            count += 1
    base.db.commit()
    logger.info("Cleaned %d deleted objects", count)
