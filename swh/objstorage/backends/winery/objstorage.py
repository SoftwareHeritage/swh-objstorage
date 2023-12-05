# Copyright (C) 2022-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from multiprocessing import Process
import time
from typing import Callable, Optional

from typing_extensions import Literal

from swh.objstorage import exc
from swh.objstorage.interface import ObjId
from swh.objstorage.objstorage import ObjStorage

from .roshard import ROShard, ROShardCreator
from .rwshard import RWShard
from .sharedbase import ShardState, SharedBase
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
        raise PermissionError("Delete is not allowed.")

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

    def roshard(self, name):
        if name not in self.ro_shards:
            shard = ROShard(name, **self.args)
            self.ro_shards[name] = shard
            if name in self.rw_shards:
                del self.rw_shards[name]
        return self.ro_shards[name]

    def rwshard(self, name):
        if name not in self.rw_shards:
            shard = RWShard(name, **self.args)
            self.rw_shards[name] = shard
        return self.rw_shards[name]

    def get(self, obj_id: ObjId) -> bytes:
        shard_info = self.base.get(obj_id)
        if shard_info is None:
            raise exc.ObjNotFoundError(obj_id)
        name, state = shard_info
        if state.readonly_available:
            shard = self.roshard(name)
            content = shard.get(obj_id)
            del shard
        else:
            shard = self.rwshard(name)
            content = shard.get(obj_id)
        if content is None:
            raise exc.ObjNotFoundError(obj_id)
        return content


def pack(shard, shared_base=None, **kwargs):
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
    rw.drop()
    shared_base.set_shard_state(name=shard, new_state=ShardState.READONLY)
    if uninit_base:
        shared_base.uninit()
    return True


class WineryWriter(WineryReader):
    def __init__(self, **kwargs):
        self.pack_immediately = kwargs.get("pack_immediately", True)
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

    def check(self, obj_id: ObjId) -> None:
        # load all shards packing == True and not locked (i.e. packer
        # was interrupted for whatever reason) run pack for each of them
        pass

    def pack(self):
        self.base.shard_packing_starts(self.shard.name)
        p = Process(target=pack, args=(self.shard.name,), kwargs=self.args)
        self.uninit()
        p.start()
        self.packers.append(p)
        self.init()

    def __del__(self):
        for p in self.packers:
            p.kill()
            p.join()


def never_stop_packing(_: int) -> bool:
    return False


def stop_after_shards(max_shards_packed: int) -> Callable[[int], bool]:
    def stop_packing(shards_packed: int):
        return shards_packed >= max_shards_packed

    return stop_packing


def sleep_exponential(min_duration: float, factor: float, max_duration: float):
    """Return a function that sleeps `min_duration`,
    then increases that by `factor` at every call, up to `max_duration`."""
    duration = min(min_duration, max_duration)

    if duration <= 0:
        raise ValueError("Cannot sleep for a negative amount of time")

    def sleep():
        nonlocal duration
        logger.debug("No shards to pack, waiting for %s", duration)
        time.sleep(duration)

        duration *= factor
        if duration >= max_duration:
            duration = max_duration

    return sleep


def shard_packer(
    base_dsn: str,
    shard_dsn: str,
    shard_max_size: int,
    throttle_read: int,
    throttle_write: int,
    output_dir: Optional[str] = None,
    stop_packing: Callable[[int], bool] = never_stop_packing,
    wait_for_shard: Callable[[], None] = sleep_exponential(
        min_duration=5, factor=2, max_duration=60
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
      output_dir: output directory for statistics
      stop_packing: callback to determine whether the packer should exit
      wait_for_shard: callback called when no shards are available to be packed
    """
    base = SharedBase(base_dsn=base_dsn)

    shards_packed = 0
    while not stop_packing(shards_packed):
        shard_to_pack = base.lock_one_shard(
            current_state=ShardState.FULL, new_state=ShardState.PACKING
        )

        if not shard_to_pack:
            wait_for_shard()
            continue

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
        )
        if not ret:
            raise ValueError("Packing shard %s failed" % name)
        shards_packed += 1

    return shards_packed
