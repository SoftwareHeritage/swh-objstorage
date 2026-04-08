# Copyright (C) 2021-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter
import logging
import threading
import time

import pytest
import yaml

import swh.objstorage.backends.winery.cli
from swh.objstorage.backends.winery.housekeeping import (
    cleanup_rw_shard,
    deleted_objects_cleaner,
    pack,
    rw_shard_cleaner,
    shard_packer,
    stop_after_shards,
)
import swh.objstorage.backends.winery.objstorage
from swh.objstorage.backends.winery.objstorage import WineryObjStorage
from swh.objstorage.backends.winery.sharedbase import ShardState, SharedBase
from swh.objstorage.backends.winery.sleep import sleep_exponential
from swh.objstorage.backends.winery.throttler import (
    BandwidthCalculator,
    IOThrottler,
    LeakyBucket,
    Throttler,
)
from swh.objstorage.cli import swh_cli_group
from swh.objstorage.exc import ObjNotFoundError, ReadOnlyObjStorageError
from swh.objstorage.factory import get_objstorage
from swh.objstorage.objstorage import objid_for_content
from swh.objstorage.tests.objstorage_testing import ObjStorageTestFixture

logger = logging.getLogger(__name__)


class TestWinery:
    def test_winery_sharedbase(self, winery_writer):
        base = winery_writer.base
        shard1 = winery_writer.shard.name
        assert shard1 is not None
        assert shard1 == base.locked_shard

        id1 = base.locked_shard_id
        assert id1 is not None
        assert id1 == base.locked_shard_id

        assert base.get_shard_state(shard1) == ShardState.WRITING

        winery_writer.release_shard()

        assert winery_writer.base._locked_shard is None
        assert base.get_shard_state(shard1) == ShardState.STANDBY

        shard2 = winery_writer.base.locked_shard

        assert shard1 == shard2, "Locked a different shard?"
        assert base.get_shard_state(shard1) == ShardState.WRITING

    def test_winery_add_get(self, winery_writer, winery_reader):
        shard = winery_writer.base.locked_shard
        content = b"SOMETHING"
        sha256 = objid_for_content(content)["sha256"]
        assert (
            sha256.hex()
            == "866878b165607851782d8d233edf0c261172ff67926330d3bbd10c705b92d24f"
        )
        winery_writer.add(content=content, obj_id=sha256)
        winery_writer.add(content=content, obj_id=sha256)
        assert winery_writer.base.locked_shard == shard
        assert winery_reader.get(sha256) == content
        with pytest.raises(ObjNotFoundError):
            winery_reader.get(b"unknown")
        winery_writer.shard.drop()

    @pytest.mark.parametrize(
        (),
        [
            pytest.param(marks=pytest.mark.use_throttler(False), id="throttler=False"),
            pytest.param(marks=pytest.mark.use_throttler(True), id="throttler=True"),
        ],
    )
    def test_winery_add_concurrent(self, winery_settings, mocker):
        num_threads = 4

        class ManualReleaseSharedBase(SharedBase):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.release_obj_id = threading.Event()

            def record_new_obj_id(self, *args, **kwargs):
                ret = super().record_new_obj_id(*args, **kwargs)
                self.release_obj_id.wait()
                return ret

        mocker.patch(
            "swh.objstorage.backends.winery.objstorage.SharedBase",
            ManualReleaseSharedBase,
        )

        content = b"test_concurrency"
        obj_id = objid_for_content(content)

        def add_object(my_storage):
            my_storage.add(content=content, obj_id=obj_id)

            assert my_storage.get(obj_id) == content

        storages = [
            get_objstorage(cls="winery", **winery_settings) for _ in range(num_threads)
        ]

        threads = [
            threading.Thread(target=add_object, args=[storage]) for storage in storages
        ]
        for thread in threads:
            thread.start()

        for storage in reversed(storages):
            storage.writer.base.release_obj_id.set()

        for thread in threads:
            thread.join()

        assert storage.reader.get(obj_id["sha256"]) == content
        assert sum(1 for _ in storage.reader.base.list_shards()) >= num_threads

        for storage in storages:
            assert isinstance(storage, WineryObjStorage)
            storage.on_shutdown()

    @pytest.mark.shard_max_size(1)
    @pytest.mark.parametrize(
        (),
        [
            pytest.param(marks=pytest.mark.use_throttler(False), id="throttler=False"),
            pytest.param(marks=pytest.mark.use_throttler(True), id="throttler=True"),
        ],
    )
    def test_winery_add_and_pack(self, winery_writer, mocker):
        mocker.patch(
            "swh.objstorage.backends.winery.objstorage.pack", return_value=True
        )
        shard = winery_writer.base.locked_shard
        content = b"SOMETHING"
        sha256 = objid_for_content(content)["sha256"]
        winery_writer.add(content=content, obj_id=sha256)
        assert winery_writer.base.locked_shard != shard
        assert len(winery_writer.packers) == 1
        packer = winery_writer.packers[0]
        packer.join()
        assert packer.exitcode == 0

    def test_winery_delete_on_rwshard(self, winery_writer, winery_reader):
        shard = winery_writer.base.locked_shard
        content = b"SOMETHING"
        sha256 = objid_for_content(content)["sha256"]
        winery_writer.add(content=content, obj_id=sha256)
        assert winery_writer.base.locked_shard == shard
        assert winery_reader.get(sha256) == content
        winery_writer.delete(sha256)
        with pytest.raises(ObjNotFoundError):
            winery_reader.get(sha256)

    @pytest.mark.shard_max_size(1)
    @pytest.mark.pack_immediately(True)
    def test_winery_delete_on_roshard(self, winery_writer, winery_reader, image_pool):
        shard = winery_writer.base.locked_shard
        content = b"SOMETHING"
        sha256 = objid_for_content(content)["sha256"]
        winery_writer.add(content=content, obj_id=sha256)
        assert winery_writer.base.locked_shard != shard
        assert winery_writer.packers
        for packer in winery_writer.packers:
            packer.join()
        assert winery_reader.get(sha256) == content

        # This will only mark as deleted in SharedBase
        winery_writer.delete(sha256)
        assert len(list(winery_writer.base.deleted_objects())) == 1
        # We still should not be able to access it
        with pytest.raises(ObjNotFoundError):
            winery_reader.get(sha256)

        # Make sure all images are released
        winery_reader.on_shutdown()

        # The content is still present in the roshard image at this point
        image_path = image_pool.image_path(shard)
        with open(image_path, "rb") as image:
            assert b"SOMETHING" in image.read()

        # Perform cleanup
        image_pool.image_unmap(shard)
        image_pool.image_map(shard, "rw")

        deleted_objects_cleaner(
            winery_reader.base, image_pool, stop_running=lambda: False
        )

        assert len(list(winery_reader.base.deleted_objects())) == 0
        with open(image_path, "rb") as image:
            assert b"SOMETHING" not in image.read()

    @pytest.mark.shard_max_size(20)
    @pytest.mark.pack_immediately(True)
    def test_winery_deleted_objects_cleaner_handles_exception(
        self, winery_writer, image_pool, mocker
    ):
        from swh.objstorage.backends.winery import housekeeping
        from swh.objstorage.backends.winery import objstorage as winery_objstorage
        from swh.objstorage.backends.winery.roshard import ROShard

        # Add two objects
        shard = winery_writer.base.locked_shard
        content1 = b"PINOT GRIS"
        sha256_1 = objid_for_content(content1)["sha256"]
        winery_writer.add(content=content1, obj_id=sha256_1)
        content2 = b"CHARDONNAY"
        sha256_2 = objid_for_content(content2)["sha256"]
        winery_writer.add(content=content2, obj_id=sha256_2)

        # This should be enough bytes to trigger packing
        for packer in winery_writer.packers:
            packer.join()

        # We should only have one roshard
        assert len(image_pool.image_list()) == 1

        # This will only mark as deleted in SharedBase for the time being
        winery_writer.delete(sha256_1)
        winery_writer.delete(sha256_2)
        assert len(list(winery_writer.base.deleted_objects())) == 2

        # The content is still present in the roshard image at this point
        image_path = image_pool.image_path(shard)

        # Setup so we get an exception on the second object
        already_called = False
        orig_roshard_delete = ROShard.delete

        def roshard_delete_side_effect(pool, shard_name, obj_id):
            nonlocal already_called
            print(already_called)
            if already_called:
                raise OSError("Unable to write to pool")
            orig_roshard_delete(pool, shard_name, obj_id)
            already_called = True
            return None

        mocker.patch.object(
            winery_objstorage.roshard.ROShard,
            "delete",
            side_effect=roshard_delete_side_effect,
        )

        # Let’s run the cleaner
        image_pool.image_unmap(shard)
        image_pool.image_map(shard, "rw")

        with pytest.raises(OSError):
            housekeeping.deleted_objects_cleaner(
                winery_writer.base, image_pool, stop_running=lambda: False
            )

        # We should only have one remaining object to delete
        assert len(list(winery_writer.base.deleted_objects())) == 1

        # We should have only the content of one of the objects still in the roshard
        with open(image_path, "rb") as image:
            image_content = image.read()
            presences = [content1 in image_content, content2 in image_content]
            assert sorted(presences) == [False, True]

    def test_winery_get_shard_info(self, winery_reader):
        assert winery_reader.base.get_shard_info(1234) is None
        assert winery_reader.base.get_shard_state("nothing") is None

    def test_winery_base_record_shard_mapped(self, winery_writer):
        # Lock a shard
        shard_name, shard_id = winery_writer.base.create_shard(
            new_state=ShardState.PACKED
        )

        assert {"test"} == winery_writer.base.record_shard_mapped(
            host="test", name=shard_name
        )
        assert {"test"} == winery_writer.base.record_shard_mapped(
            host="test", name=shard_name
        )
        assert {"test", "test2"} == winery_writer.base.record_shard_mapped(
            host="test2", name=shard_name
        )

    @pytest.mark.shard_max_size(10 * 1024 * 1024)
    @pytest.mark.clean_immediately(False)
    def test_winery_pack(self, winery_settings, winery_writer, image_pool):
        shard = winery_writer.base.locked_shard
        content = b"SOMETHING"
        sha256 = objid_for_content(content)["sha256"]
        winery_writer.add(content=content, obj_id=sha256)
        winery_writer.base.set_shard_state(ShardState.FULL)
        winery_writer.base.shard_packing_starts(shard)

        assert pack(
            shard=shard,
            base_dsn=winery_settings["database"]["db"],
            packer_settings=winery_settings["packer"],
            throttler_settings=winery_settings["throttler"],
            shards_settings=winery_settings["shards"],
            shards_pool_settings=winery_settings["shards_pool"],
        )
        assert winery_writer.base.get_shard_state(shard) == ShardState.PACKED

        assert cleanup_rw_shard(shard, base_dsn=winery_settings["database"]["db"])
        assert winery_writer.base.get_shard_state(shard) == ShardState.READONLY

    @pytest.mark.shard_max_size(1024 * 1024)
    @pytest.mark.pack_immediately(True)
    def test_winery_writer_pack_immediately_true(self, image_pool, storage):
        shard = storage.writer.base.locked_shard

        for i in range(1024):
            content = i.to_bytes(1024, "little")
            obj_id = objid_for_content(content)
            storage.add(content=content, obj_id=obj_id)

        assert storage.writer.packers
        for packer in storage.writer.packers:
            packer.join()

        assert storage.writer.base.locked_shard != shard

        assert storage.writer.base.get_shard_state(shard) == ShardState.READONLY

    @pytest.mark.shard_max_size(300 * 1024)
    @pytest.mark.pack_immediately(True)
    def test_winery_readonly_storage(self, image_pool, storage, readonly_storage):
        for i in range(1024):
            content = i.to_bytes(1024, "little")
            obj_id = objid_for_content(content)
            storage.add(content=content, obj_id=obj_id)

        assert storage.writer.packers
        for packer in storage.writer.packers:
            packer.join()

        for i in range(1024):
            content = i.to_bytes(1024, "little")
            obj_id = objid_for_content(content)
            assert readonly_storage.get(obj_id=obj_id) == content

        # Check that some RW shards were used
        assert readonly_storage.reader.rw_shards

        content = (1025).to_bytes(1024, "little")
        obj_id = objid_for_content(content)
        with pytest.raises(ReadOnlyObjStorageError):
            readonly_storage.add(content=content, obj_id=obj_id)

    @pytest.mark.shard_max_size(1024 * 1024)
    @pytest.mark.pack_immediately(False)
    def test_winery_writer_pack_immediately_false(self, storage):
        shard = storage.writer.base.locked_shard

        for i in range(1024):
            content = i.to_bytes(1024, "little")
            obj_id = objid_for_content(content)
            storage.add(content=content, obj_id=obj_id)

        assert storage.writer.base.locked_shard != shard
        assert not storage.writer.packers

        assert storage.writer.base.get_shard_state(shard) == ShardState.FULL

    @pytest.mark.parametrize(
        "min_duration,factor,max_duration,expected",
        (
            (1, 2, 10, [1, 2, 4, 8, 10, 10]),
            (10, 1.5, 20, [10, 15.0, 20.0, 20.0]),
            (20, 1.3, 10, [10, 10, 10]),
        ),
    )
    def test_winery_sleep_exponential(
        self, mocker, min_duration, factor, max_duration, expected
    ):
        calls = []

        def mocked_sleep(t: float):
            calls.append(t)

        mocker.patch("time.sleep", mocked_sleep)

        sleep = sleep_exponential(
            min_duration=min_duration,
            factor=factor,
            max_duration=max_duration,
            message="Message",
        )

        for i, _ in enumerate(expected):
            sleep(i)

        assert calls == expected

    def test_winery_sleep_exponential_negative(
        self,
    ):
        with pytest.raises(ValueError, match="negative amount"):
            _ = sleep_exponential(
                min_duration=-1, factor=2, max_duration=10, message="Message"
            )

    @pytest.mark.shard_max_size(1024)
    @pytest.mark.pack_immediately(False)
    @pytest.mark.clean_immediately(False)
    def test_winery_standalone_packer(self, winery_settings, image_pool, storage):
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

        # Pack a single shard
        assert (
            shard_packer(
                **winery_settings,
                stop_packing=stop_after_shards(1),
            )
            == 1
        )

        shard_counts = Counter(state for _, state in storage.writer.base.list_shards())
        assert shard_counts == {
            ShardState.FULL: 3,
            ShardState.PACKED: 1,
        }

        # Clean up the RW shard for the packed one
        assert (
            rw_shard_cleaner(
                database=winery_settings["database"],
                min_mapped_hosts=0,
                stop_cleaning=stop_after_shards(1),
            )
            == 1
        )

        shard_counts = Counter(state for _, state in storage.writer.base.list_shards())
        assert shard_counts == {
            ShardState.FULL: 3,
            ShardState.READONLY: 1,
        }

        # Pack all remaining shards
        assert (
            shard_packer(
                **winery_settings,
                stop_packing=stop_after_shards(3),
            )
            == 3
        )

        shard_counts = Counter(state for _, state in storage.writer.base.list_shards())
        assert shard_counts == {
            ShardState.PACKED: 3,
            ShardState.READONLY: 1,
        }

        # Clean up the RW shard for the packed one
        assert (
            rw_shard_cleaner(
                database=winery_settings["database"],
                min_mapped_hosts=0,
                stop_cleaning=stop_after_shards(3),
            )
            == 3
        )

        shard_counts = Counter(state for _, state in storage.writer.base.list_shards())
        assert shard_counts == {ShardState.READONLY: 4}

    @pytest.mark.shard_max_size(1024)
    @pytest.mark.pack_immediately(False)
    @pytest.mark.clean_immediately(False)
    def test_winery_packer_clean_up_interrupted_shard(
        self, image_pool, winery_settings, storage, caplog
    ):
        caplog.set_level(logging.CRITICAL)

        # create 1 full shard
        for i in range(4):
            content = i.to_bytes(256, "little")
            obj_id = objid_for_content(content)
            storage.add(content=content, obj_id=obj_id)

        filled = storage.writer.shards_filled
        assert len(filled) == 1

        shard = filled[0]

        if not image_pool.image_mapped(shard):
            image_pool.image_create(shard)

        with open(image_pool.image_path(shard), "wb") as f:
            f.write(b"SWHShard interrupted bla")

        with caplog.at_level(logging.WARNING, "swh.objstorage.backends.winery.roshard"):
            # Pack a single shard
            ret = shard_packer(
                database=winery_settings["database"],
                shards=winery_settings["shards"],
                shards_pool=winery_settings["shards_pool"],
                throttler=winery_settings["throttler"],
                packer={**winery_settings.get("packer"), "create_images": False},
                stop_packing=stop_after_shards(1),
            )

        assert ret == 1
        found_cleanup_message = False
        found_subprocess_error = False
        for record in caplog.records:
            msg = record.getMessage()
            if image_pool.image_path(shard) in msg:
                if "cleaning it up" in msg:
                    found_cleanup_message = True
                elif "failed:" in msg:
                    found_subprocess_error = True
        else:
            assert found_cleanup_message and not found_subprocess_error, [
                r.getMessage() for r in caplog.records
            ]

    @pytest.mark.shard_max_size(1024)
    @pytest.mark.pack_immediately(False)
    @pytest.mark.clean_immediately(False)
    def test_winery_cli_packer(
        self, image_pool, storage, tmp_path, winery_settings, cli_runner
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
            ("objstorage", "winery", "packer", "--stop-after-shards=4"),
            env={"SWH_CONFIG_FILENAME": str(tmp_path / "config.yml")},
        )

        assert result.exit_code == 0

        shard_info = dict(storage.writer.base.list_shards())
        for shard in filled:
            assert shard_info[shard] == ShardState.PACKED

    @pytest.mark.shard_max_size(1024)
    @pytest.mark.pack_immediately(False)
    @pytest.mark.clean_immediately(False)
    def test_winery_cli_packer_rollback_on_error(
        self, image_pool, storage, tmp_path, winery_settings, cli_runner
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

        # pytest-mock doesn't seem to interact very well with the cli_runner
        def failing_pack(*args, **kwargs):
            raise ValueError("Packing failed")

        orig_pack = swh.objstorage.backends.winery.objstorage.pack
        try:
            swh.objstorage.backends.winery.housekeeping.pack = failing_pack
            result = cli_runner.invoke(
                swh_cli_group,
                ("objstorage", "winery", "packer", "--stop-after-shards=4"),
                env={"SWH_CONFIG_FILENAME": str(tmp_path / "config.yml")},
            )
        finally:
            swh.objstorage.backends.winery.housekeeping.pack = orig_pack

        assert result.exit_code == 1

        shard_info = dict(storage.writer.base.list_shards())
        for shard in filled:
            assert (
                shard_info[shard] == ShardState.FULL
            ), f"{shard} in state {shard_info[shard]}"

    @pytest.mark.shard_max_size(1024)
    @pytest.mark.pack_immediately(False)
    def test_winery_cli_rbd(
        self, image_pool, storage, tmp_path, winery_settings, cli_runner
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

        # The RBD shard mapper was run in "read-only" mode
        for shard in filled:
            assert image_pool.image_mapped(shard) is None

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
                assert image_pool.image_mapped(shard) == "rw"
            else:
                assert image_pool.image_mapped(shard) is None

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
            assert image_pool.image_mapped(shard) == "rw"

        for shard in filled:
            storage.writer.base.set_shard_state(name=shard, new_state=ShardState.PACKED)

        result = cli_runner.invoke(
            swh_cli_group,
            ("objstorage", "winery", "rbd", "--stop-instead-of-waiting"),
            env={"SWH_CONFIG_FILENAME": str(tmp_path / "config.yml")},
        )

        assert result.exit_code == 0

        for shard in filled:
            assert image_pool.image_mapped(shard) == "ro"

    @pytest.mark.shard_max_size(1024)
    @pytest.mark.pack_immediately(True)
    @pytest.mark.clean_immediately(False)
    def test_winery_cli_rw_shard_cleaner(
        self, image_pool, postgresql_dsn, storage, tmp_path, winery_settings, cli_runner
    ):
        # create 4 shards
        for i in range(16):
            content = i.to_bytes(256, "little")
            obj_id = objid_for_content(content)
            storage.add(content=content, obj_id=obj_id)

        filled = storage.writer.shards_filled
        assert len(filled) == 4

        for packer in storage.writer.packers:
            packer.join()
            assert packer.exitcode == 0

        shard_info = dict(storage.writer.base.list_shards())
        for shard in filled:
            assert shard_info[shard] == ShardState.PACKED

        with open(tmp_path / "config.yml", "w") as f:
            yaml.safe_dump(
                {"objstorage": {"cls": "winery", **winery_settings}}, stream=f
            )

        shard_tables = set(storage.writer.base.list_shard_tables())
        for shard in filled:
            assert shard in shard_tables

        result = cli_runner.invoke(
            swh_cli_group,
            ("objstorage", "winery", "rw-shard-cleaner", "--stop-instead-of-waiting"),
            env={"SWH_CONFIG_FILENAME": str(tmp_path / "config.yml")},
        )
        assert result.exit_code == 0

        # No hosts have mapped the shard as remapped, so the cleaner has done nothing
        shard_tables = set(storage.writer.base.list_shard_tables())
        for shard in filled:
            assert shard in shard_tables

        result = cli_runner.invoke(
            swh_cli_group,
            (
                "objstorage",
                "winery",
                "rw-shard-cleaner",
                "--stop-instead-of-waiting",
                "--min-mapped-hosts=0",
            ),
            env={"SWH_CONFIG_FILENAME": str(tmp_path / "config.yml")},
        )

        assert result.exit_code == 0

        # Now we've forced action
        shard_tables = set(storage.writer.base.list_shard_tables())
        for shard in filled:
            assert shard not in shard_tables

    @pytest.mark.shard_max_size(1024)
    @pytest.mark.pack_immediately(True)
    @pytest.mark.clean_immediately(False)
    def test_winery_cli_rw_shard_cleaner_rollback_on_error(
        self, image_pool, postgresql_dsn, storage, tmp_path, winery_settings, cli_runner
    ):
        # create 4 shards
        for i in range(16):
            content = i.to_bytes(256, "little")
            obj_id = objid_for_content(content)
            storage.add(content=content, obj_id=obj_id)

        filled = storage.writer.shards_filled
        assert len(filled) == 4

        for packer in storage.writer.packers:
            packer.join()
            assert packer.exitcode == 0

        shard_info = dict(storage.writer.base.list_shards())
        for shard in filled:
            assert shard_info[shard] == ShardState.PACKED

        with open(tmp_path / "config.yml", "w") as f:
            yaml.safe_dump(
                {"objstorage": {"cls": "winery", **winery_settings}}, stream=f
            )

        shard_tables = set(storage.writer.base.list_shard_tables())
        for shard in filled:
            assert shard in shard_tables

        # pytest-mock doesn't seem to interact very well with the cli_runner
        def failing_cleanup(*args, **kwargs):
            raise ValueError("Cleanup failed")

        orig_cleanup = swh.objstorage.backends.winery.housekeeping.cleanup_rw_shard
        try:
            swh.objstorage.backends.winery.housekeeping.cleanup_rw_shard = (
                failing_cleanup
            )

            result = cli_runner.invoke(
                swh_cli_group,
                (
                    "objstorage",
                    "winery",
                    "rw-shard-cleaner",
                    "--stop-instead-of-waiting",
                    "--min-mapped-hosts=0",
                ),
                env={"SWH_CONFIG_FILENAME": str(tmp_path / "config.yml")},
            )
        finally:
            swh.objstorage.backends.winery.housekeeping.cleanup_rw_shard = orig_cleanup

        assert result.exit_code == 1

        shard_tables = set(storage.writer.base.list_shard_tables())
        shard_info = dict(storage.writer.base.list_shards())
        for shard in filled:
            assert shard in shard_tables
            assert shard_info[shard] == ShardState.PACKED

    @pytest.mark.shard_max_size(1024)
    @pytest.mark.pack_immediately(False)
    @pytest.mark.clean_immediately(False)
    def test_winery_standalone_packer_never_stop_packing(
        self, image_pool, postgresql_dsn, shard_max_size, storage, winery_settings
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

        class NoShardLeft(Exception):
            pass

        called = []

        def wait_five_times(attempt) -> None:
            called.append(attempt)
            if attempt >= 4:
                raise NoShardLeft(attempt)

        with pytest.raises(NoShardLeft):
            shard_packer(
                **winery_settings,
                wait_for_shard=wait_five_times,
            )

        assert called == list(range(5))

        shard_counts = Counter(state for _, state in storage.writer.base.list_shards())
        assert shard_counts == {ShardState.PACKED: 4}

        called = []

        with pytest.raises(NoShardLeft):
            rw_shard_cleaner(
                database=winery_settings["database"],
                min_mapped_hosts=0,
                wait_for_shard=wait_five_times,
            )

        assert called == list(range(5))

        shard_counts = Counter(state for _, state in storage.writer.base.list_shards())
        assert shard_counts == {ShardState.READONLY: 4}

    @pytest.mark.shard_max_size(10 * 1024 * 1024)
    def test_winery_get_object(
        self, winery_settings, winery_writer, winery_reader, image_pool
    ):
        shard = winery_writer.base.locked_shard
        content = b"SOMETHING"
        sha256 = objid_for_content(content)["sha256"]
        winery_writer.add(content=content, obj_id=sha256)
        winery_writer.base.set_shard_state(ShardState.FULL)
        winery_writer.base.shard_packing_starts(shard)
        assert (
            pack(
                shard,
                base_dsn=winery_settings["database"]["db"],
                packer_settings=winery_settings["packer"],
                throttler_settings=winery_settings["throttler"],
                shards_settings=winery_settings["shards"],
                shards_pool_settings=winery_settings["shards_pool"],
            )
            is True
        )
        assert winery_reader.get(sha256) == content

    def test_winery_leaky_bucket_tick(self, mocker):
        total = 100
        half = 50
        b = LeakyBucket(total)
        sleep = mocker.spy(time, "sleep")
        assert b.current == b.total
        sleep.assert_not_called()
        #
        # Bucket is at 100, add(50) => drops to 50
        #
        b.add(half)
        assert b.current == half
        sleep.assert_not_called()
        #
        # Bucket is at 50, add(50) => drops to 0
        #
        b.add(half)
        assert b.current == 0
        sleep.assert_not_called()
        #
        # Bucket is at 0, add(50) => waits until it is at 50 and then drops to 0
        #
        b.add(half)
        assert b.current == 0
        sleep.assert_called_once()
        #
        # Sleep more than one second, bucket is full again, i.e. at 100
        #
        time.sleep(2)
        mocker.resetall()
        b.add(0)
        assert b.current == total
        sleep.assert_not_called()
        #
        # Bucket is full at 100 and and waits when requesting 150 which is
        # more than it can contain
        #
        b.add(total + half)
        assert b.current == 0
        sleep.assert_called_once()
        mocker.resetall()
        #
        # Bucket is empty and and waits when requesting 150 which is more
        # than it can contain
        #
        b.add(total + half)
        assert b.current == 0
        sleep.assert_called_once()
        mocker.resetall()

    def test_winery_leaky_bucket_reset(
        self,
    ):
        b = LeakyBucket(100)
        assert b.total == 100
        assert b.current == b.total
        b.reset(50)
        assert b.total == 50
        assert b.current == b.total
        b.reset(100)
        assert b.total == 100
        assert b.current == 50

    def test_winery_bandwidth_calculator(self, mocker):
        now = 1

        def monotonic():
            return now

        mocker.patch("time.monotonic", side_effect=monotonic)
        b = BandwidthCalculator()
        assert b.get() == 0
        count = 100 * 1024 * 1024
        going_up = []
        for t in range(b.duration):
            now += 1
            b.add(count)
            going_up.append(b.get())
        assert b.get() == count
        going_down = []
        for t in range(b.duration - 1):
            now += 1
            b.add(0)
            going_down.append(b.get())
        going_down.reverse()
        assert going_up[:-1] == going_down
        assert len(b.history) == b.duration - 1

    def test_winery_io_throttler(self, postgresql_dsn, mocker):
        sleep = mocker.spy(time, "sleep")
        speed = 100
        i = IOThrottler(name="read", db=postgresql_dsn, max_speed=100)
        count = speed
        i.add(count)
        sleep.assert_not_called()
        i.add(count)
        sleep.assert_called_once()
        #
        # Force slow down
        #
        mocker.resetall()
        i.sync_interval = 0
        i.max_speed = 1
        assert i.max_speed != i.bucket.total
        i.add(2)
        assert i.max_speed == i.bucket.total
        sleep.assert_called_once()

    def test_winery_throttler(self, postgresql_dsn):
        t = Throttler(
            db=postgresql_dsn,
            max_write_bps=100,
            max_read_bps=100,
        )

        base = {}
        key = "KEY"
        content = "CONTENT"

        def reader(k):
            return base[k]

        def writer(k, v):
            base[k] = v
            return True

        assert t.throttle_add(writer, key, content) is True
        assert t.throttle_get(reader, key) == content


class TestWineryObjStorage(ObjStorageTestFixture):
    @pytest.fixture(autouse=True)
    def objstorage(self, storage):
        self.storage = storage

    @pytest.mark.skip("This interface is not supported as such by winery")
    def test_restore_content(self):
        pass

    @pytest.mark.skip("Winery has more extended signatures than the interface expects")
    def test_types(self):
        pass
