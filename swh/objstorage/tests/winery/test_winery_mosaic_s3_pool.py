# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os

import boto3
import pytest

from swh.mosaic import CompressionMethod, IdxDescription
from swh.objstorage.backends.winery.pools.mosaic import MosaicS3BackedPool
from swh.objstorage.objstorage import objid_for_content

from .test_objstorage_winery_mosaic import shards  # noqa
from .test_winery_cli import invoke
from .winery_objstorage_testing import TestWinery as _TestWinery
from .winery_objstorage_testing import TestWineryObjStorage as _TestWineryObjStorage

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def moto_server():
    """Fixture to run a mocked AWS server for testing."""
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
def mosaic_s3_config(moto_server):
    s3_bucket = "softwareheritage"

    # configure rust aws client to use moto server
    os.environ["AWS_ALLOW_HTTP"] = "true"
    os.environ["AWS_ENDPOINT"] = moto_server
    os.environ["AWS_SECRET_ACCESS_KEY"] = "foo"
    os.environ["AWS_ACCESS_KEY_ID"] = "bar"

    boto3_config = {
        "endpoint_url": moto_server,
        "aws_access_key_id": "bar",
        "aws_secret_access_key": "foo",
    }

    s3 = boto3.resource("s3", **boto3_config)
    bucket = s3.Bucket(name=s3_bucket)
    bucket.create()

    yield f"s3://{s3_bucket}", boto3_config

    bucket.objects.all().delete()
    bucket.delete()


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
    s3_uri, boto3_config = mosaic_s3_config
    pools = []
    for pool_name in pool_names:
        if pool_name.endswith("-mosaic-s3"):
            pool = MosaicS3BackedPool(
                base_url=s3_uri,
                tmp_dir=tmp_path,
                shard_max_size=shard_max_size,
                pool_name=pool_name,
                compression_level=3,
                boto3_config=boto3_config,
            )
            pool.image_unmap_all()
            pool._settings_for_tests = {
                "type": "mosaic-s3",
                "base_url": s3_uri,
                "tmp_dir": str(tmp_path),
                "pool_name": pool_name,
                "compression_level": 3,
                "shard_max_size": shard_max_size,
                "boto3_config": boto3_config,
            }
        else:
            raise ValueError(f"Unsupported pool name: {pool_name}")

        pools.append(pool)

    yield pools


class TestWineryMosaicS3Pool:

    def test_packing(self, storage, winery_settings, shard_max_size, mosaic_s3_config):
        mosaic_s3_uri, boto3_config = mosaic_s3_config
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

        client = boto3.client("s3", **boto3_config)
        # mosaic files should be uploaded to s3 now
        for shard, _ in shard_list:
            image_path = pool.image_path(shard)
            assert image_path.startswith("s3://")
            assert client.head_object(
                Bucket="softwareheritage", Key=f"{pool_name}/{shard}"
            )
            img = pool.image_open(shard)
            assert img.indexes == [IdxDescription.SHA256FMPHGO]
            assert len(img) == 10
            assert img.compression == CompressionMethod.ZSTD

        # we should be able to retrieve all the objects
        for objid in objids:
            assert storage.get(objid)


class TestMosaicS3WineryObjStorage(_TestWineryObjStorage): ...


class TestMosaicS3Winery(_TestWinery): ...
