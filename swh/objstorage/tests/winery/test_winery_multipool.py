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


class TestWineryMultipoolObjStorage(_TestWineryObjStorage):
    pass


class TestMultipoolWinery(_TestWinery):
    pass
