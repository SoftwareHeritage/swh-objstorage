# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os

from swh.objstorage.backends.winery.housekeeping import import_ro_shards
from swh.shard import Shard

logger = logging.getLogger(__name__)


def test_import_ro_shards(storage, shards):
    pool = next(iter(storage.pools.values()))
    pooldir = pool.base_directory
    poolname = pool.pool_name
    for shard in shards:
        name = os.path.basename(shard)
        os.link(shard, os.path.join(pooldir, poolname, name))

    for shard, objids in shards.items():
        for objid in objids:
            assert objid not in storage

    n_objs, n_shards = import_ro_shards(storage.writer.base, pool)
    assert n_shards == 6
    assert n_objs == 12 * 6

    for shard, objids in shards.items():
        for objid in objids:
            assert objid in storage
    # ensure newly imported shards are registered in the right pool
    for shard in shards:
        name = os.path.basename(shard)
        assert storage.reader.base.get_shard_pool(name) == poolname


def test_import_ro_shards_w_existing_objects(storage, shards):
    pool = next(iter(storage.pools.values()))
    pooldir = pool.base_directory
    poolname = pool.pool_name
    for shard in shards:
        name = os.path.basename(shard)
        os.link(shard, os.path.join(pooldir, poolname, name))

    shardpath, objids = next(iter(shards.items()))
    shard = Shard(shardpath)
    existing_objs = []
    for objid in objids[::2]:
        storage.add(shard[objid["sha256"]], objid)
        existing_objs.append(objid)

    base = storage.writer.base
    n_objs, n_shards = import_ro_shards(base, pool)
    assert n_shards == 6
    assert n_objs == 12 * 5 + 6

    for shard, objids in shards.items():
        for objid in objids:
            assert objid in storage
    for objid in existing_objs:
        shardname, state = base.get_shard_info(base.contains(objid["sha256"]))
        assert shardname != os.path.basename(shardpath)
