# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from multiprocessing import Process

from swh.model import hashutil
from swh.objstorage import exc
from swh.objstorage.objstorage import ObjStorage

from .roshard import ROShard
from .rwshard import RWShard
from .sharedbase import SharedBase
from .stats import Stats

logger = logging.getLogger(__name__)


def compute_hash(content):
    algo = "sha256"
    return hashutil.MultiHash.from_data(content, hash_names=[algo],).digest().get(algo)


class WineryObjStorage(ObjStorage):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if kwargs.get("readonly"):
            self.winery = WineryReader(**kwargs)
        else:
            self.winery = WineryWriter(**kwargs)

    def uninit(self):
        self.winery.uninit()

    def get(self, obj_id):
        return self.winery.get(obj_id)

    def check_config(self, *, check_write):
        return True

    def __contains__(self, obj_id):
        return obj_id in self.winery

    def add(self, content, obj_id=None, check_presence=True):
        return self.winery.add(content, obj_id, check_presence)

    def check(self, obj_id):
        return self.winery.check(obj_id)

    def delete(self, obj_id):
        raise PermissionError("Delete is not allowed.")


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
        self.shards = {}

    def roshard(self, name):
        if name not in self.shards:
            shard = ROShard(name, **self.args)
            shard.load()
            self.shards[name] = shard
        return self.shards[name]

    def get(self, obj_id):
        shard_info = self.base.get(obj_id)
        if shard_info is None:
            raise exc.ObjNotFoundError(obj_id)
        name, readonly = shard_info
        if readonly:
            shard = self.roshard(name)
            content = shard.get(obj_id)
            del shard
        else:
            shard = RWShard(name, **self.args)
            content = shard.get(obj_id)
        if content is None:
            raise exc.ObjNotFoundError(obj_id)
        return content


def pack(shard, **kwargs):
    return Packer(shard, **kwargs).run()


class Packer:
    def __init__(self, shard, **kwargs):
        self.stats = Stats(kwargs.get("output_dir"))
        self.args = kwargs
        self.shard = shard
        self.init()

    def init(self):
        self.rw = RWShard(self.shard, **self.args)
        self.ro = ROShard(self.shard, **self.args)

    def uninit(self):
        del self.ro
        self.rw.uninit()

    def run(self):
        self.ro.create(self.rw.count())
        for obj_id, content in self.rw.all():
            self.ro.add(content, obj_id)
            if self.stats.stats_active:
                self.stats.stats_read(obj_id, content)
                self.stats.stats_write(obj_id, content)
        self.ro.save()
        base = SharedBase(**self.args)
        base.shard_packing_ends(self.shard)
        base.uninit()
        self.rw.uninit()
        self.rw.drop()
        return True


class WineryWriter(WineryReader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.packers = []
        self.init()

    def init(self):
        super().init()
        self.shard = RWShard(self.base.whoami, **self.args)

    def uninit(self):
        self.shard.uninit()
        super().uninit()

    def add(self, content, obj_id=None, check_presence=True):
        if obj_id is None:
            obj_id = compute_hash(content)

        if check_presence and obj_id in self:
            return obj_id

        shard = self.base.add_phase_1(obj_id)
        if shard != self.base.id:
            #  this object is the responsibility of another shard
            return obj_id

        self.shard.add(obj_id, content)
        self.base.add_phase_2(obj_id)

        if self.shard.is_full():
            self.pack()

        return obj_id

    def check(self, obj_id):
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
