# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

import boto3
import pytest

from swh.mosaic import CompressionMethod, IdxDescription
from swh.objstorage.backends.winery.pools.mosaic import MosaicS3BackedPool, s3_to_boto3
from swh.objstorage.objstorage import objid_for_content
from swh.objstorage.tests.test_objstorage_s3 import (  # noqa
    CONTAINER_NAME,
    moto_container,
    moto_server,
)

from .test_objstorage_winery_mosaic import shards  # noqa
from .test_winery_cli import invoke
from .winery_objstorage_testing import TestWinery as _TestWinery
from .winery_objstorage_testing import TestWineryObjStorage as _TestWineryObjStorage

logger = logging.getLogger(__name__)


@pytest.fixture
def mosaic_s3_config(moto_container):  # noqa
    s3_bucket = moto_container["container_name"]

    s3_config = {
        "aws_endpoint_url": "http://{host}:{port}".format(**moto_container),
        "aws_access_key_id": moto_container["key"],
        "aws_secret_access_key": moto_container["secret"],
        "aws_allow_http": True,
    }

    yield f"s3://{s3_bucket}", s3_config


@pytest.fixture
def pool_names(request, pytestconfig):
    return [
        "winery-pool-01-active-mosaic-s3",
    ]


@pytest.fixture
def image_pools(
    tmp_path,
    shard_max_size,
    pool_names,
    mosaic_s3_config,
):
    s3_uri, s3_config = mosaic_s3_config
    pools = []
    for pool_name in pool_names:
        if pool_name.endswith("-mosaic-s3"):
            pool = MosaicS3BackedPool(
                base_url=s3_uri,
                tmp_directory=tmp_path,
                shard_max_size=shard_max_size,
                pool_name=pool_name,
                compression_level=3,
                s3_config=s3_config,
            )
            pool.image_unmap_all()
            pool._settings_for_tests = {
                "type": "mosaic-s3",
                "base_url": s3_uri,
                "tmp_directory": str(tmp_path),
                "pool_name": pool_name,
                "compression_level": 3,
                "shard_max_size": shard_max_size,
                "s3_config": s3_config,
            }
        else:
            raise ValueError(f"Unsupported pool name: {pool_name}")

        pools.append(pool)

    yield pools


class TestWineryMosaicS3Pool:

    def test_packing(self, storage, winery_settings, shard_max_size, mosaic_s3_config):
        mosaic_s3_uri, s3_config = mosaic_s3_config
        nbytes = shard_max_size // 10
        # create 5 shards in the pool
        objids = []
        for n in range(5):
            for i in range(10):
                content = b"s3/%d/%d " % (n, i) + b"\x00" * nbytes
                content = content[: nbytes + 1]
                objid = objid_for_content(content)
                storage.add(content, objid)
                objids.append(objid)

        # we should have 5 full shards a this point...
        base = storage.writer.base
        shard_list = list(base.list_shards())
        assert len(shard_list) == 5
        assert all(state.name == "FULL" for shard, state in shard_list)

        # now we want to run the packer
        result = invoke(
            "winery",
            "packer",
            "--stop-instead-of-waiting",
            config=winery_settings,
        )
        assert result.exit_code == 0

        # all shards from current active pool should be packed
        pool_name = winery_settings["shards_active_pool"]
        pool = storage.pools[pool_name]
        shard_list = list(base.list_shards())
        assert len(shard_list) == 5
        packed = [state.name == "PACKED" for shard, state in shard_list]
        assert len(packed) == 5
        assert all(packed)

        # local files should not exists any more at this point
        assert list((pool.tmp_dir / pool.pool_name).iterdir()) == []

        # ensure database tables have been dropped
        result = invoke(
            "winery",
            "rw-shard-cleaner",
            "--stop-instead-of-waiting",
            config=winery_settings,
        )
        assert result.exit_code == 0

        boto3_config = s3_to_boto3(s3_config)
        client = boto3.client("s3", **boto3_config)
        # mosaic files should be uploaded to s3 now
        for shard, _ in shard_list:
            image_path = pool.image_path(shard)
            assert image_path.startswith("s3://")
            assert client.head_object(Bucket=CONTAINER_NAME, Key=f"{pool_name}/{shard}")
            img = pool.image_open(shard)
            assert img.indexes == [IdxDescription.SHA256FMPHGO]
            assert len(img) == 10
            assert img.compression == CompressionMethod.ZSTD

        # we should be able to retrieve all the objects
        for objid in objids:
            assert storage.get(objid)


class TestMosaicS3WineryObjStorage(_TestWineryObjStorage): ...


class TestMosaicS3Winery(_TestWinery): ...
