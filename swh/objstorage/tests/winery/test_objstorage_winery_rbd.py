# Copyright (C) 2021-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os

import pytest
import yaml

from swh.objstorage.backends.winery.sharedbase import ShardState
from swh.objstorage.cli import swh_cli_group
from swh.objstorage.objstorage import objid_for_content

from .winery_objstorage_testing import TestWinery as _TestWinery
from .winery_objstorage_testing import TestWineryObjStorage as _TestWineryObjStorage
from .winery_testing_helpers import RBDPoolHelper

logger = logging.getLogger(__name__)


@pytest.fixture
def pool_names():
    return ["winery-pool-active-rbd"]


class TestCephWineryObjStorage(_TestWineryObjStorage):
    pass


class TestCephWinery(_TestWinery):
    @pytest.mark.skipif(
        "CEPH_HARDCODE_POOL" in os.environ, reason="Ceph pool hardcoded"
    )
    def test_winery_ceph_pool(self, needs_ceph):
        name = "IMAGE"
        pool = RBDPoolHelper(
            shard_max_size=10 * 1024 * 1024,
            rbd_pool_name="test-winery-ceph-pool",
            rbd_map_options=os.environ.get("RBD_MAP_OPTIONS", ""),
        )
        pool.remove()
        pool.pool_create()
        assert pool.image_mapped(name) is None
        pool.image_create(name)
        assert pool.image_mapped(name) == "rw"
        p = pool.image_path(name)
        assert p.endswith(name)
        something = "SOMETHING"
        with open(p, "w") as f:
            f.write(something)
        with open(p) as f:
            assert f.read(len(something)) == something
        assert pool.image_list() == [name]
        pool.image_remap_ro(name)
        if pool.image_mapped(name) == "rw":
            raise ValueError(
                "Remapping image read-only kept write permissions. "
                "Are the udev rules properly installed?"
            )
        assert pool.image_mapped(name) == "ro"
        pool.images_remove()
        assert pool.image_list() == []
        assert pool.image_mapped(name) is None
        pool.remove()
        assert pool.image_list() == []

    @pytest.mark.shard_max_size(1024)
    def test_winery_cli_rbd(
        self, write_pool_name, storage, tmp_path, winery_settings, cli_runner
    ):
        # create 4 shards
        for i in range(16):
            content = i.to_bytes(256, "little")
            obj_id = objid_for_content(content)
            storage.add(content=content, obj_id=obj_id)

        filled = storage.writer.shards_filled
        assert len(filled) == 4

        shard_info = dict(storage.writer.base.list_shards())
        for shard in filled:
            assert shard_info[shard] == ShardState.FULL

        with open(tmp_path / "config.yml", "w") as f:
            yaml.safe_dump(
                {"objstorage": {"cls": "winery", **winery_settings}}, stream=f
            )

        result = cli_runner.invoke(
            swh_cli_group,
            (
                "objstorage",
                "winery",
                "rbd",
                "--stop-instead-of-waiting",
            ),
            env={"SWH_CONFIG_FILENAME": str(tmp_path / "config.yml")},
        )

        assert result.exit_code == 0
        image_write_pool = storage.pools[write_pool_name]
        # The RBD shard mapper was run in "read-only" mode
        for shard in filled:
            assert image_write_pool.image_mapped(shard) is None

        first_shard = filled[0]

        result = cli_runner.invoke(
            swh_cli_group,
            (
                "objstorage",
                "winery",
                "rbd",
                "--stop-instead-of-waiting",
                "--only-prefix",
                first_shard[:10],
                "--manage-rw-images",
            ),
            env={"SWH_CONFIG_FILENAME": str(tmp_path / "config.yml")},
        )

        assert result.exit_code == 0

        for shard in filled:
            if shard == first_shard:
                assert image_write_pool.image_mapped(shard) == "rw"
            else:
                assert image_write_pool.image_mapped(shard) is None

        result = cli_runner.invoke(
            swh_cli_group,
            (
                "objstorage",
                "winery",
                "rbd",
                "--stop-instead-of-waiting",
                "--manage-rw-images",
            ),
            env={"SWH_CONFIG_FILENAME": str(tmp_path / "config.yml")},
        )

        assert result.exit_code == 0
        for shard in filled:
            assert image_write_pool.image_mapped(shard) == "rw"

        for shard in filled:
            storage.writer.base.set_shard_state(name=shard, new_state=ShardState.PACKED)

        result = cli_runner.invoke(
            swh_cli_group,
            ("objstorage", "winery", "rbd", "--stop-instead-of-waiting"),
            env={"SWH_CONFIG_FILENAME": str(tmp_path / "config.yml")},
        )

        assert result.exit_code == 0

        for shard in filled:
            assert image_write_pool.image_mapped(shard) == "ro"
