# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os

import pytest

from swh.mosaic import MosaicReader
from swh.objstorage.backends.winery.sharedbase import ShardState

from .test_winery_cli import invoke

logger = logging.getLogger(__name__)


@pytest.fixture
def pool_names(request, pytestconfig):
    return [
        "winery-pool-01-active-directory",
        "winery-pool-02-mosaic",
    ]


def test_winery_migrate_shards(storage, winery_settings, write_pool_name, shards):

    from_pool = "winery-pool-01-active-directory"
    to_pool = "winery-pool-02-mosaic"
    active_pool = storage.pools[from_pool]
    pooldir = active_pool.base_directory / from_pool

    for shard in shards:
        name = os.path.basename(shard)
        os.link(shard, pooldir / name)

    result = invoke("winery", "import-shards", config=winery_settings)
    assert result.exit_code == 0

    result = invoke(
        "winery",
        "migrate-pool",
        from_pool,
        to_pool,
        "--no-progress",
        config=winery_settings,
    )
    assert result.exit_code == 0

    base = storage.writer.base
    dstpool = storage.pools[to_pool]

    for img, status in base.list_shards():
        assert status == ShardState.READONLY
        assert base.get_shard_pool(img) == "winery-pool-02-mosaic"
        assert os.path.isfile(dstpool.image_path(img))
        assert isinstance(dstpool.image_open(img), MosaicReader)


def test_winery_migrate_shards_options(
    storage, winery_settings, write_pool_name, shards
):

    from_pool = "winery-pool-01-active-directory"
    to_pool = "winery-pool-02-mosaic"
    active_pool = storage.pools[from_pool]
    pooldir = active_pool.base_directory / from_pool

    for shard in shards:
        name = os.path.basename(shard)
        os.link(shard, pooldir / name)

    result = invoke("winery", "import-shards", config=winery_settings)
    assert result.exit_code == 0

    base = storage.writer.base
    images = list(img[0] for img in base.list_shards())
    image = images[0]
    dstpool = storage.pools[to_pool]

    # test --image options
    result = invoke(
        "winery",
        "migrate-pool",
        from_pool,
        to_pool,
        "--image",
        "i_dont_exists",
        config=winery_settings,
    )
    assert result.exit_code == 0
    assert "Failed to open 'i_dont_exists', skipping" in result.output

    result = invoke(
        "winery",
        "migrate-pool",
        from_pool,
        to_pool,
        "--image",
        image,
        config=winery_settings,
    )
    assert result.exit_code == 0

    for img in images:
        if img == image:
            assert base.get_shard_pool(img) == to_pool
            assert os.path.isfile(dstpool.image_path(img))
            assert isinstance(dstpool.image_open(img), MosaicReader)
        else:
            assert base.get_shard_pool(img) == from_pool
            assert not os.path.isfile(dstpool.image_path(img))

    # test --limit option
    result = invoke(
        "winery",
        "migrate-pool",
        from_pool,
        to_pool,
        "--limit",
        2,
        config=winery_settings,
    )
    assert result.exit_code == 0

    assert len(os.listdir(dstpool.image_path(""))) == 3

    # test --no-db-update
    result = invoke(
        "winery",
        "migrate-pool",
        from_pool,
        to_pool,
        "--limit",
        1,
        "--no-db-update",
        config=winery_settings,
    )
    assert result.exit_code == 0
    assert len(os.listdir(dstpool.image_path(""))) == 4
    in_dst = [img for img in images if base.get_shard_pool(img) == to_pool]
    assert len(in_dst) == 3  # still considered in from_pool

    # test --no-transfer-image
    result = invoke(
        "winery",
        "migrate-pool",
        from_pool,
        to_pool,
        "--no-transfer-images",
        config=winery_settings,
    )
    assert result.exit_code == 0
    assert len(os.listdir(dstpool.image_path(""))) == 4
    in_dst = [img for img in images if base.get_shard_pool(img) == to_pool]
    assert len(in_dst) == 4  # db should have updated now
