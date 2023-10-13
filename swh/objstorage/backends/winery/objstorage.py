# Copyright (C) 2022-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from multiprocessing import Process
import time

from typing_extensions import Literal

from swh.objstorage import exc
from swh.objstorage.interface import ObjId
from swh.objstorage.objstorage import ObjStorage

from .roshard import ROShard
from .rwshard import RWShard
from .sharedbase import SharedBase
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
            shard.load()
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
        name, readonly = shard_info
        if readonly:
            shard = self.roshard(name)
            content = shard.get(obj_id)
            del shard
        else:
            shard = self.rwshard(name)
            content = shard.get(obj_id)
        if content is None:
            raise exc.ObjNotFoundError(obj_id)
        return content


def pack(shard, **kwargs):
    stats = Stats(kwargs.get("output_dir"))
    rw = RWShard(shard, **kwargs)
    ro = ROShard(shard, **kwargs)

    count = rw.count()
    logger.info("Creating RO shard %s for %s objects", shard, count)
    ro.create(count)
    logger.info("Created RO shard %s", shard)
    for i, (obj_id, content) in enumerate(rw.all()):
        ro.add(content, obj_id)
        if stats.stats_active:
            stats.stats_read(obj_id, content)
            stats.stats_write(obj_id, content)
        if i % 100 == 99:
            logger.debug("RO shard %s: added %s/%s objects", shard, i + 1, count)

    logger.debug("RO shard %s: added %s objects, saving", shard, count)
    assert ro.save() != -1, f"Shard saving failed for {shard}"
    logger.debug("RO shard %s: saved", shard)

    while True:
        if ro.load():
            break

        logger.warn("Shard %s didn't sync yet, sleeping", shard)
        time.sleep(0.1)
        ro = ROShard(shard, **kwargs)

    base = SharedBase(**kwargs)
    base.shard_packing_ends(shard)
    base.uninit()
    rw.uninit()
    rw.drop()
    return True


class WineryWriter(WineryReader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.packers = []

    def init(self):
        super().init()
        self.shard = RWShard(self.base.whoami, **self.args)
        logger.debug("WineryBase: RWShard %s instantiated", self.base.whoami)

    def uninit(self):
        self.shard.uninit()
        super().uninit()

    def add(self, content: bytes, obj_id: ObjId, check_presence: bool = True) -> None:
        if check_presence and obj_id in self:
            return

        shard = self.base.add_phase_1(obj_id)
        if shard != self.base.id:
            #  this object is the responsibility of another shard
            return

        self.shard.add(obj_id, content)
        self.base.add_phase_2(obj_id)

        if self.shard.is_full():
            self.pack()

    def check(self, obj_id: ObjId) -> None:
        # load all shards packing == True and not locked (i.e. packer
        # was interrupted for whatever reason) run pack for each of them
        pass

    def pack(self):
        self.base.shard_packing_starts()
        p = Process(target=pack, args=(self.shard.name,), kwargs=self.args)
        self.uninit()
        p.start()
        self.packers.append(p)
        self.init()

    def __del__(self):
        for p in self.packers:
            p.kill()
            p.join()
