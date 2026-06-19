# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from itertools import cycle
import logging
import os

import pytest

from swh.objstorage.backends.winery.housekeeping import import_ro_shards
from swh.objstorage.backends.winery.settings import populate_default_settings
from swh.objstorage.factory import get_objstorage
from swh.objstorage.objstorage import objid_for_content

from .test_winery_cli import invoke
from .winery_objstorage_testing import TestWinery as _TestWinery
from .winery_objstorage_testing import TestWineryObjStorage as _TestWineryObjStorage

logger = logging.getLogger(__name__)


@pytest.fixture
def pool_names(request, pytestconfig):
    return [
        "winery-pool-01-directory",
        "winery-pool-02-active-directory",
        "winery-pool-03-directory",
    ]


@pytest.fixture
def storage(winery_settings, shards):
    """A multipool (obj)storage fixture that will feed RO pools with premade shards"""
    storage = get_objstorage(cls="winery", **winery_settings)
    # fill non-active pools with random shards
    for pool, shard in zip(
        cycle(p for p in storage.pools.values() if "-active-" not in p.pool_name),
        shards,
    ):
        shardname = os.path.basename(shard)
        pooldir = pool.base_directory / pool.pool_name
        os.link(shard, pooldir / shardname)
        import_ro_shards(storage.writer.base, pool)

    yield storage
    storage.on_shutdown()


class TestWineryMultiPool:
    def test_pools_config(self, winery_settings):
        assert populate_default_settings(**winery_settings)

        # no shards_active_pool is set
        cfg = populate_default_settings(
            **{**winery_settings, "shards_active_pool": None}
        )
        assert "shards_active_pool" not in cfg

        # at least one pool is required
        with pytest.raises(ValueError):
            populate_default_settings(
                **{
                    **winery_settings,
                    "shards_pools": [],
                    "shards_active_pool": None,
                }
            )

        # active pool name must be an existing pool
        with pytest.raises(ValueError):
            populate_default_settings(
                **{**winery_settings, "shards_active_pool": "no a pool"}
            )

    def test_packing(self, winery_settings, pool_names, shard_max_size):

        nbytes = shard_max_size // 10
        for npool, pool_name in enumerate(pool_names):
            # create a few shards in this pool
            storage = get_objstorage(
                cls="winery", **{**winery_settings, "shards_active_pool": pool_name}
            )
            # creates 5 shards of 10 objects
            for n in range(5):
                for i in range(10):
                    content = b"%d/%d/%d " % (npool, n, i) + b"\x00" * nbytes
                    content = content[: nbytes + 1]
                    objid = objid_for_content(content)
                    storage.add(content, objid)

        # we should have 5*len(pool_names) full shards a this point...
        base = storage.writer.base
        shards = list(base.list_shards())
        assert len(shards) == 5 * len(pool_names)
        assert all(state.name == "FULL" for shard, state in shards)
        pools = [base.get_shard_pool(shard) for shard, state in shards]
        assert sorted(pools) == sorted(pool_names * 5)

        # now we want to run the packer
        result = invoke(
            "winery",
            "packer",
            "--stop-instead-of-waiting",
            config=winery_settings,
        )
        assert result.exit_code == 0

        # all shards from current active pool should be packed
        active_pool = winery_settings["shards_active_pool"]
        shards = list(base.list_shards())
        assert len(shards) == 5 * len(pool_names)
        packed = [
            state.name == "PACKED"
            for shard, state in shards
            if base.get_shard_pool(shard) == active_pool
        ]
        assert len(packed) == 5
        assert all(packed)
        assert all(
            state.name == "FULL"
            for shard, state in shards
            if base.get_shard_pool(shard) != active_pool
        )

        # run the packer for a specified pool (other then the configured active one)
        pool = pool_names[-1]
        assert pool != active_pool

        result = invoke(
            "winery",
            "packer",
            "--stop-instead-of-waiting",
            "--pool-name",
            pool,
            config=winery_settings,
        )
        assert result.exit_code == 0
        shards = list(base.list_shards())
        assert len(shards) == 5 * len(pool_names)
        packed = [
            state.name == "PACKED"
            for shard, state in shards
            if base.get_shard_pool(shard) == pool
        ]
        assert len(packed) == 5
        assert all(packed)
        assert all(
            state.name == "FULL"
            for shard, state in shards
            if base.get_shard_pool(shard) not in (pool, active_pool)
        )

        # run the packer for a all the pools
        result = invoke(
            "winery",
            "packer",
            "--stop-instead-of-waiting",
            "--pool-name",
            "all",
            config=winery_settings,
        )
        assert result.exit_code == 0
        shards = list(base.list_shards())
        assert len(shards) == 5 * len(pool_names)
        packed = [state.name == "PACKED" for shard, state in shards]
        assert len(packed) == 5 * len(pool_names)
        assert all(packed)

        # each shard file should be stored in the correct pool
        for shard, _ in shards:
            pool_name = base.get_shard_pool(shard)
            pool = storage.pools[pool_name]
            assert os.path.exists(pool.image_path(shard))


class TestWineryMultipoolObjStorage(_TestWineryObjStorage):
    pass


class TestMultipoolWinery(_TestWinery):
    pass
