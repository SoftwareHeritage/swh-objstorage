# Copyright (C) 2021-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter
from functools import partial
import logging
import os
import shutil
import threading
import time

from click.testing import CliRunner
import pytest
from pytest_postgresql import factories
import yaml

from swh.core.db.db_utils import initialize_database_for_module
import swh.objstorage.backends.winery.objstorage
from swh.objstorage.backends.winery.objstorage import (
    WineryObjStorage,
    cleanup_rw_shard,
    deleted_objects_cleaner,
    pack,
    rw_shard_cleaner,
    shard_packer,
    stop_after_shards,
)
from swh.objstorage.backends.winery.sharedbase import ShardState, SharedBase
from swh.objstorage.backends.winery.sleep import sleep_exponential
from swh.objstorage.backends.winery.throttler import (
    BandwidthCalculator,
    IOThrottler,
    LeakyBucket,
    Throttler,
)
from swh.objstorage.cli import swh_cli_group
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.factory import get_objstorage
from swh.objstorage.objstorage import objid_for_content
from swh.objstorage.tests.objstorage_testing import ObjStorageTestFixture

from .winery_testing_helpers import FileBackedPool, PoolHelper

logger = logging.getLogger(__name__)


@pytest.fixture
def needs_ceph():
    ceph = shutil.which("ceph")

    if not ceph:
        pytest.skip("the ceph CLI was not found")
    if os.environ.get("USE_CEPH", "no") != "yes":
        pytest.skip(
            "the ceph-based tests have been disabled (USE_CEPH env var is not 'yes')"
        )


@pytest.fixture
def cli_runner(capsys):
    "Run click commands with log capture disabled"

    class CapsysDisabledCliRunner(CliRunner):
        def invoke(self, *args, **kwargs):
            with capsys.disabled():
                return super().invoke(*args, **kwargs)

    return CapsysDisabledCliRunner()


@pytest.fixture
def remove_pool(request, pytestconfig):
    if os.environ.get("CEPH_HARDCODE_POOL"):
        return False
    else:
        return True


@pytest.fixture
def remove_images(request, pytestconfig):
    if os.environ.get("CEPH_HARDCODE_POOL"):
        return False
    else:
        return True


@pytest.fixture
def rbd_pool_name(request, pytestconfig):
    if os.environ.get("CEPH_HARDCODE_POOL"):
        return os.environ["CEPH_HARDCODE_POOL"]
    else:
        return "winery-test-shards"


@pytest.fixture
def rbd_map_options():
    return os.environ.get("RBD_MAP_OPTIONS", "")


@pytest.fixture
def ceph_pool(remove_pool, remove_images, rbd_pool_name, rbd_map_options, needs_ceph):
    pool = PoolHelper(
        shard_max_size=10 * 1024 * 1024,
        rbd_pool_name=rbd_pool_name,
        rbd_map_options=rbd_map_options,
    )
    if remove_pool:
        pool.remove()
        pool.pool_create()
    else:
        logger.info("Not removing pool")

    yield pool

    if remove_images or remove_pool:
        pool.images_remove()
    else:
        logger.info("Not removing images")

    if remove_pool:
        pool.remove()
    else:
        logger.info("Not removing pool")


@pytest.fixture
def file_backed_pool(mocker, tmp_path, shard_max_size, rbd_pool_name):
    FileBackedPool.set_base_directory(tmp_path)
    mocker.patch(
        "swh.objstorage.backends.winery.roshard.Pool",
        new=FileBackedPool,
    )
    pool = FileBackedPool(shard_max_size=10 * 1024 * 1024, rbd_pool_name=rbd_pool_name)
    pool.image_unmap_all()
    yield pool


def pytest_generate_tests(metafunc):
    if "image_pool" in metafunc.fixturenames:
        metafunc.parametrize(
            "image_pool", ["ceph_pool", "file_backed_pool"], indirect=True
        )


@pytest.fixture
def image_pool(request):
    return request.getfixturevalue(request.param)


winery_postgresql_proc = factories.postgresql_proc(
    load=[
        partial(
            initialize_database_for_module,
            modname="objstorage.backends.winery",
            version=SharedBase.current_version,
        ),
    ],
)

winery_postgresql = factories.postgresql("winery_postgresql_proc")


@pytest.fixture
def postgresql_dsn(winery_postgresql):
    return winery_postgresql.info.dsn


@pytest.fixture
def shard_max_size(request) -> int:
    marker = request.node.get_closest_marker("shard_max_size")
    if marker is None:
        return 1024
    else:
        return marker.args[0]


@pytest.fixture
def pack_immediately(request) -> bool:
    marker = request.node.get_closest_marker("pack_immediately")
    if marker is None:
        return True
    else:
        return marker.args[0]


@pytest.fixture
def clean_immediately(request) -> bool:
    marker = request.node.get_closest_marker("clean_immediately")
    if marker is None:
        return True
    else:
        return marker.args[0]


@pytest.fixture
def storage(
    shard_max_size,
    pack_immediately,
    clean_immediately,
    rbd_pool_name,
    rbd_map_options,
    postgresql_dsn,
):
    storage = get_objstorage(
        cls="winery",
        base_dsn=postgresql_dsn,
        shard_max_size=shard_max_size,
        throttle_write=200 * 1024 * 1024,
        throttle_read=100 * 1024 * 1024,
        pack_immediately=pack_immediately,
        clean_immediately=clean_immediately,
        rbd_pool_name=rbd_pool_name,
        rbd_map_options=rbd_map_options,
    )
    assert isinstance(storage, WineryObjStorage)
    logger.debug("Instantiated storage %s on rbd pool %s", storage, rbd_pool_name)
    yield storage
    storage.on_shutdown()
    names = [
        thread.name
        for thread in threading.enumerate()
        if thread.name.startswith("IdleHandler")
    ]
    assert not names, f"Some IdleHandlers are still alive: {','.join(names)}"


@pytest.fixture
def winery(storage):
    return storage.winery


def test_winery_sharedbase(winery):
    base = winery.base
    shard1 = winery.shard.name
    assert shard1 is not None
    assert shard1 == base.locked_shard

    id1 = base.locked_shard_id
    assert id1 is not None
    assert id1 == base.locked_shard_id

    assert base.get_shard_state(shard1) == ShardState.WRITING

    winery.release_shard()

    assert winery.base._locked_shard is None
    assert base.get_shard_state(shard1) == ShardState.STANDBY

    shard2 = winery.base.locked_shard

    assert shard1 == shard2, "Locked a different shard?"
    assert base.get_shard_state(shard1) == ShardState.WRITING


def test_winery_add_get(winery):
    shard = winery.base.locked_shard
    content = b"SOMETHING"
    sha256 = objid_for_content(content)["sha256"]
    assert (
        sha256.hex()
        == "866878b165607851782d8d233edf0c261172ff67926330d3bbd10c705b92d24f"
    )
    winery.add(content=content, obj_id=sha256)
    winery.add(content=content, obj_id=sha256)
    winery.add(content=content, obj_id=sha256, check_presence=False)
    assert winery.base.locked_shard == shard
    assert winery.get(sha256) == content
    with pytest.raises(ObjNotFoundError):
        winery.get(b"unknown")
    winery.shard.drop()


def test_winery_add_concurrent(winery, mocker):
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
        "swh.objstorage.backends.winery.objstorage.SharedBase", ManualReleaseSharedBase
    )

    content = b"test_concurrency"
    obj_id = objid_for_content(content)

    def add_object(my_storage):
        my_storage.add(content=content, obj_id=obj_id)

        assert my_storage.get(obj_id) == content

    storages = [get_objstorage(cls="winery", **winery.args) for _ in range(num_threads)]

    threads = [
        threading.Thread(target=add_object, args=[storage]) for storage in storages
    ]
    for thread in threads:
        thread.start()

    for storage in reversed(storages):
        storage.winery.base.release_obj_id.set()

    for thread in threads:
        thread.join()

    assert winery.get(obj_id["sha256"]) == content
    assert sum(1 for _ in winery.base.list_shards()) >= num_threads

    for storage in storages:
        assert isinstance(storage, WineryObjStorage)
        storage.on_shutdown()


@pytest.mark.shard_max_size(1)
def test_winery_add_and_pack(winery, mocker):
    mocker.patch("swh.objstorage.backends.winery.objstorage.pack", return_value=True)
    shard = winery.base.locked_shard
    content = b"SOMETHING"
    sha256 = objid_for_content(content)["sha256"]
    winery.add(content=content, obj_id=sha256)
    assert winery.base.locked_shard != shard
    assert len(winery.packers) == 1
    packer = winery.packers[0]
    packer.join()
    assert packer.exitcode == 0


def test_winery_delete_on_rwshard(winery):
    shard = winery.base.locked_shard
    content = b"SOMETHING"
    sha256 = objid_for_content(content)["sha256"]
    winery.add(content=content, obj_id=sha256)
    assert winery.base.locked_shard == shard
    assert winery.get(sha256) == content
    winery.delete(sha256)
    with pytest.raises(ObjNotFoundError):
        winery.get(sha256)


@pytest.mark.shard_max_size(1)
@pytest.mark.pack_immediately(True)
def test_winery_delete_on_roshard(winery, file_backed_pool):
    shard = winery.base.locked_shard
    content = b"SOMETHING"
    sha256 = objid_for_content(content)["sha256"]
    winery.add(content=content, obj_id=sha256)
    assert winery.base.locked_shard != shard
    assert winery.packers
    for packer in winery.packers:
        packer.join()
    assert winery.get(sha256) == content
    # This will only mark as deleted in SharedBase
    winery.delete(sha256)
    assert len(list(winery.base.deleted_objects())) == 1
    # We still should not be able to access it
    with pytest.raises(ObjNotFoundError):
        winery.get(sha256)
    # The content is still present in the roshard image at this point
    image_path = file_backed_pool.image_path(shard)
    with open(image_path, "rb") as image:
        assert b"SOMETHING" in image.read()
    # Perform cleanup
    file_backed_pool.image_unmap(shard)
    file_backed_pool.image_map(shard, "rw")
    deleted_objects_cleaner(winery.base, file_backed_pool, stop_running=lambda: False)
    assert len(list(winery.base.deleted_objects())) == 0
    with open(image_path, "rb") as image:
        assert b"SOMETHING" not in image.read()


@pytest.mark.shard_max_size(20)
@pytest.mark.pack_immediately(True)
def test_winery_deleted_objects_cleaner_handles_exception(
    winery, file_backed_pool, mocker
):
    from swh.objstorage.backends.winery import objstorage as winery_objstorage

    from ..backends.winery.roshard import ROShard

    # Add two objects
    shard = winery.base.locked_shard
    content1 = b"PINOT GRIS"
    sha256_1 = objid_for_content(content1)["sha256"]
    winery.add(content=content1, obj_id=sha256_1)
    content2 = b"CHARDONNAY"
    sha256_2 = objid_for_content(content2)["sha256"]
    winery.add(content=content2, obj_id=sha256_2)

    # This should be enough bytes to trigger packing
    for packer in winery.packers:
        packer.join()

    # We should only have one roshard
    assert len(file_backed_pool.image_list()) == 1

    # This will only mark as deleted in SharedBase for the time being
    winery.delete(sha256_1)
    winery.delete(sha256_2)
    assert len(list(winery.base.deleted_objects())) == 2

    # The content is still present in the roshard image at this point
    image_path = file_backed_pool.image_path(shard)

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
        winery_objstorage.ROShard, "delete", side_effect=roshard_delete_side_effect
    )

    # Let’s run the cleaner
    file_backed_pool.image_unmap(shard)
    file_backed_pool.image_map(shard, "rw")
    with pytest.raises(OSError):
        winery_objstorage.deleted_objects_cleaner(
            winery.base, file_backed_pool, stop_running=lambda: False
        )

    # We should only have one remaining object to delete
    assert len(list(winery.base.deleted_objects())) == 1

    # We should have only the content of one of the objects still in the roshard
    with open(image_path, "rb") as image:
        image_content = image.read()
        presences = [content1 in image_content, content2 in image_content]
        assert sorted(presences) == [False, True]


def test_winery_get_shard_info(winery):
    assert winery.base.get_shard_info(1234) is None
    assert winery.base.get_shard_state("nothing") is None


def test_winery_base_record_shard_mapped(winery):
    # Lock a shard
    shard_name, shard_id = winery.base.create_shard(new_state=ShardState.PACKED)

    assert {"test"} == winery.base.record_shard_mapped(host="test", name=shard_name)
    assert {"test"} == winery.base.record_shard_mapped(host="test", name=shard_name)
    assert {"test", "test2"} == winery.base.record_shard_mapped(
        host="test2", name=shard_name
    )


@pytest.mark.shard_max_size(10 * 1024 * 1024)
@pytest.mark.clean_immediately(False)
def test_winery_pack(winery, image_pool):
    shard = winery.base.locked_shard
    content = b"SOMETHING"
    sha256 = objid_for_content(content)["sha256"]
    winery.add(content=content, obj_id=sha256)
    winery.base.set_shard_state(ShardState.FULL)
    winery.base.shard_packing_starts(shard)

    assert pack(shard, **winery.args)
    assert winery.base.get_shard_state(shard) == ShardState.PACKED

    assert cleanup_rw_shard(shard, **winery.args)
    assert winery.base.get_shard_state(shard) == ShardState.READONLY


@pytest.mark.shard_max_size(1024 * 1024)
@pytest.mark.pack_immediately(True)
def test_winery_writer_pack_immediately_true(image_pool, storage):
    shard = storage.winery.base.locked_shard

    for i in range(1024):
        content = i.to_bytes(1024, "little")
        obj_id = objid_for_content(content)
        storage.add(content=content, obj_id=obj_id)

    assert storage.winery.packers
    for packer in storage.winery.packers:
        packer.join()

    assert storage.winery.base.locked_shard != shard

    assert storage.winery.base.get_shard_state(shard) == ShardState.READONLY


@pytest.mark.shard_max_size(1024 * 1024)
@pytest.mark.pack_immediately(False)
def test_winery_writer_pack_immediately_false(storage):
    shard = storage.winery.base.locked_shard

    for i in range(1024):
        content = i.to_bytes(1024, "little")
        obj_id = objid_for_content(content)
        storage.add(content=content, obj_id=obj_id)

    assert storage.winery.base.locked_shard != shard
    assert not storage.winery.packers

    assert storage.winery.base.get_shard_state(shard) == ShardState.FULL


@pytest.mark.parametrize(
    "min_duration,factor,max_duration,expected",
    (
        (1, 2, 10, [1, 2, 4, 8, 10, 10]),
        (10, 1.5, 20, [10, 15.0, 20.0, 20.0]),
        (20, 1.3, 10, [10, 10, 10]),
    ),
)
def test_winery_sleep_exponential(mocker, min_duration, factor, max_duration, expected):
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


def test_winery_sleep_exponential_negative():
    with pytest.raises(ValueError, match="negative amount"):
        _ = sleep_exponential(
            min_duration=-1, factor=2, max_duration=10, message="Message"
        )


@pytest.mark.shard_max_size(1024)
@pytest.mark.pack_immediately(False)
def test_winery_standalone_packer(shard_max_size, image_pool, postgresql_dsn, storage):
    # create 4 shards
    for i in range(16):
        content = i.to_bytes(256, "little")
        obj_id = objid_for_content(content)
        storage.add(content=content, obj_id=obj_id)

    filled = storage.winery.shards_filled
    assert len(filled) == 4

    shard_info = dict(storage.winery.base.list_shards())
    for shard in filled:
        assert shard_info[shard] == ShardState.FULL

    # Pack a single shard
    assert (
        shard_packer(
            base_dsn=postgresql_dsn,
            shard_max_size=shard_max_size,
            throttle_read=200 * 1024 * 1024,
            throttle_write=200 * 1024 * 1024,
            stop_packing=stop_after_shards(1),
            rbd_pool_name=image_pool.pool_name,
            rbd_map_options=image_pool.map_options,
        )
        == 1
    )

    shard_counts = Counter(state for _, state in storage.winery.base.list_shards())
    assert shard_counts == {
        ShardState.FULL: 3,
        ShardState.PACKED: 1,
    }

    # Clean up the RW shard for the packed one
    assert (
        rw_shard_cleaner(
            base_dsn=postgresql_dsn,
            min_mapped_hosts=0,
            stop_cleaning=stop_after_shards(1),
        )
        == 1
    )

    shard_counts = Counter(state for _, state in storage.winery.base.list_shards())
    assert shard_counts == {
        ShardState.FULL: 3,
        ShardState.READONLY: 1,
    }

    # Pack all remaining shards
    assert (
        shard_packer(
            base_dsn=postgresql_dsn,
            shard_max_size=shard_max_size,
            throttle_read=200 * 1024 * 1024,
            throttle_write=200 * 1024 * 1024,
            stop_packing=stop_after_shards(3),
            rbd_pool_name=image_pool.pool_name,
            rbd_map_options=image_pool.map_options,
        )
        == 3
    )

    shard_counts = Counter(state for _, state in storage.winery.base.list_shards())
    assert shard_counts == {
        ShardState.PACKED: 3,
        ShardState.READONLY: 1,
    }

    # Clean up the RW shard for the packed one
    assert (
        rw_shard_cleaner(
            base_dsn=postgresql_dsn,
            min_mapped_hosts=0,
            stop_cleaning=stop_after_shards(3),
        )
        == 3
    )

    shard_counts = Counter(state for _, state in storage.winery.base.list_shards())
    assert shard_counts == {ShardState.READONLY: 4}


@pytest.mark.shard_max_size(1024)
@pytest.mark.pack_immediately(False)
def test_winery_packer_clean_up_interrupted_shard(
    shard_max_size, image_pool, postgresql_dsn, storage, caplog
):
    caplog.set_level(logging.CRITICAL)

    # create 1 full shard
    for i in range(4):
        content = i.to_bytes(256, "little")
        obj_id = objid_for_content(content)
        storage.add(content=content, obj_id=obj_id)

    filled = storage.winery.shards_filled
    assert len(filled) == 1

    shard = filled[0]

    if not image_pool.image_mapped(shard):
        image_pool.image_create(shard)

    with open(image_pool.image_path(shard), "wb") as f:
        f.write(b"SWHShard interrupted bla")

    with caplog.at_level(logging.WARNING, "swh.objstorage.backends.winery.roshard"):
        # Pack a single shard
        ret = shard_packer(
            base_dsn=postgresql_dsn,
            shard_max_size=shard_max_size,
            throttle_read=200 * 1024 * 1024,
            throttle_write=200 * 1024 * 1024,
            stop_packing=stop_after_shards(1),
            rbd_pool_name=image_pool.pool_name,
            rbd_create_images=False,
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
def test_winery_cli_packer(image_pool, storage, tmp_path, cli_runner):
    # create 4 shards
    for i in range(16):
        content = i.to_bytes(256, "little")
        obj_id = objid_for_content(content)
        storage.add(content=content, obj_id=obj_id)

    filled = storage.winery.shards_filled
    assert len(filled) == 4

    shard_info = dict(storage.winery.base.list_shards())
    for shard in filled:
        assert shard_info[shard] == ShardState.FULL

    with open(tmp_path / "config.yml", "w") as f:
        yaml.safe_dump(
            {"objstorage": {"cls": "winery", **storage.winery.args}}, stream=f
        )

    result = cli_runner.invoke(
        swh_cli_group,
        ("objstorage", "winery", "packer", "--stop-after-shards=4"),
        env={"SWH_CONFIG_FILENAME": str(tmp_path / "config.yml")},
    )

    assert result.exit_code == 0

    shard_info = dict(storage.winery.base.list_shards())
    for shard in filled:
        assert shard_info[shard] == ShardState.PACKED


@pytest.mark.shard_max_size(1024)
@pytest.mark.pack_immediately(False)
@pytest.mark.clean_immediately(False)
def test_winery_cli_packer_rollback_on_error(image_pool, storage, tmp_path, cli_runner):
    # create 4 shards
    for i in range(16):
        content = i.to_bytes(256, "little")
        obj_id = objid_for_content(content)
        storage.add(content=content, obj_id=obj_id)

    filled = storage.winery.shards_filled
    assert len(filled) == 4

    shard_info = dict(storage.winery.base.list_shards())
    for shard in filled:
        assert shard_info[shard] == ShardState.FULL

    with open(tmp_path / "config.yml", "w") as f:
        yaml.safe_dump(
            {"objstorage": {"cls": "winery", **storage.winery.args}}, stream=f
        )

    # pytest-mock doesn't seem to interact very well with the cli_runner
    def failing_pack(*args, **kwargs):
        raise ValueError("Packing failed")

    orig_pack = swh.objstorage.backends.winery.objstorage.pack
    try:
        swh.objstorage.backends.winery.objstorage.pack = failing_pack
        result = cli_runner.invoke(
            swh_cli_group,
            ("objstorage", "winery", "packer", "--stop-after-shards=4"),
            env={"SWH_CONFIG_FILENAME": str(tmp_path / "config.yml")},
        )
    finally:
        swh.objstorage.backends.winery.objstorage.pack = orig_pack

    assert result.exit_code == 1

    shard_info = dict(storage.winery.base.list_shards())
    for shard in filled:
        assert (
            shard_info[shard] == ShardState.FULL
        ), f"{shard} in state {shard_info[shard]}"


@pytest.mark.shard_max_size(1024)
@pytest.mark.pack_immediately(False)
def test_winery_cli_rbd(image_pool, storage, tmp_path, cli_runner):
    # create 4 shards
    for i in range(16):
        content = i.to_bytes(256, "little")
        obj_id = objid_for_content(content)
        storage.add(content=content, obj_id=obj_id)

    filled = storage.winery.shards_filled
    assert len(filled) == 4

    shard_info = dict(storage.winery.base.list_shards())
    for shard in filled:
        assert shard_info[shard] == ShardState.FULL

    with open(tmp_path / "config.yml", "w") as f:
        yaml.safe_dump(
            {"objstorage": {"cls": "winery", **storage.winery.args}}, stream=f
        )

    result = cli_runner.invoke(
        swh_cli_group,
        ("objstorage", "winery", "rbd", "--stop-instead-of-waiting"),
        env={"SWH_CONFIG_FILENAME": str(tmp_path / "config.yml")},
    )

    assert result.exit_code == 0

    for shard in filled:
        assert image_pool.image_mapped(shard) == "rw"

    for shard in filled:
        storage.winery.base.set_shard_state(name=shard, new_state=ShardState.PACKED)

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
    image_pool, postgresql_dsn, storage, tmp_path, cli_runner
):
    # create 4 shards
    for i in range(16):
        content = i.to_bytes(256, "little")
        obj_id = objid_for_content(content)
        storage.add(content=content, obj_id=obj_id)

    filled = storage.winery.shards_filled
    assert len(filled) == 4

    for packer in storage.winery.packers:
        packer.join()
        assert packer.exitcode == 0

    shard_info = dict(storage.winery.base.list_shards())
    for shard in filled:
        assert shard_info[shard] == ShardState.PACKED

    with open(tmp_path / "config.yml", "w") as f:
        yaml.safe_dump(
            {"objstorage": {"cls": "winery", **storage.winery.args}}, stream=f
        )

    shard_tables = set(storage.winery.base.list_shard_tables())
    for shard in filled:
        assert shard in shard_tables

    result = cli_runner.invoke(
        swh_cli_group,
        ("objstorage", "winery", "rw-shard-cleaner", "--stop-instead-of-waiting"),
        env={"SWH_CONFIG_FILENAME": str(tmp_path / "config.yml")},
    )

    assert result.exit_code == 0

    # No hosts have mapped the shard as remapped, so the cleaner has done nothing
    shard_tables = set(storage.winery.base.list_shard_tables())
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
    shard_tables = set(storage.winery.base.list_shard_tables())
    for shard in filled:
        assert shard not in shard_tables


@pytest.mark.shard_max_size(1024)
@pytest.mark.pack_immediately(True)
@pytest.mark.clean_immediately(False)
def test_winery_cli_rw_shard_cleaner_rollback_on_error(
    image_pool, postgresql_dsn, storage, tmp_path, cli_runner
):
    # create 4 shards
    for i in range(16):
        content = i.to_bytes(256, "little")
        obj_id = objid_for_content(content)
        storage.add(content=content, obj_id=obj_id)

    filled = storage.winery.shards_filled
    assert len(filled) == 4

    for packer in storage.winery.packers:
        packer.join()
        assert packer.exitcode == 0

    shard_info = dict(storage.winery.base.list_shards())
    for shard in filled:
        assert shard_info[shard] == ShardState.PACKED

    with open(tmp_path / "config.yml", "w") as f:
        yaml.safe_dump(
            {"objstorage": {"cls": "winery", **storage.winery.args}}, stream=f
        )

    shard_tables = set(storage.winery.base.list_shard_tables())
    for shard in filled:
        assert shard in shard_tables

    # pytest-mock doesn't seem to interact very well with the cli_runner
    def failing_cleanup(*args, **kwargs):
        raise ValueError("Cleanup failed")

    orig_cleanup = swh.objstorage.backends.winery.objstorage.cleanup_rw_shard
    try:
        swh.objstorage.backends.winery.objstorage.cleanup_rw_shard = failing_cleanup

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
        swh.objstorage.backends.winery.objstorage.cleanup_rw_shard = orig_cleanup

    assert result.exit_code == 1

    shard_tables = set(storage.winery.base.list_shard_tables())
    shard_info = dict(storage.winery.base.list_shards())
    for shard in filled:
        assert shard in shard_tables
        assert shard_info[shard] == ShardState.PACKED


@pytest.mark.shard_max_size(1024)
@pytest.mark.pack_immediately(False)
def test_winery_standalone_packer_never_stop_packing(
    image_pool, postgresql_dsn, shard_max_size, storage
):
    # create 4 shards
    for i in range(16):
        content = i.to_bytes(256, "little")
        obj_id = objid_for_content(content)
        storage.add(content=content, obj_id=obj_id)

    filled = storage.winery.shards_filled
    assert len(filled) == 4

    shard_info = dict(storage.winery.base.list_shards())
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
            base_dsn=postgresql_dsn,
            shard_max_size=shard_max_size,
            throttle_read=200 * 1024 * 1024,
            throttle_write=200 * 1024 * 1024,
            wait_for_shard=wait_five_times,
            rbd_pool_name=image_pool.pool_name,
            rbd_map_options=image_pool.map_options,
        )

    assert called == list(range(5))

    shard_counts = Counter(state for _, state in storage.winery.base.list_shards())
    assert shard_counts == {ShardState.PACKED: 4}

    called = []

    with pytest.raises(NoShardLeft):
        rw_shard_cleaner(
            base_dsn=postgresql_dsn,
            min_mapped_hosts=0,
            wait_for_shard=wait_five_times,
        )

    assert called == list(range(5))

    shard_counts = Counter(state for _, state in storage.winery.base.list_shards())
    assert shard_counts == {ShardState.READONLY: 4}


@pytest.mark.shard_max_size(10 * 1024 * 1024)
def test_winery_get_object(winery, image_pool):
    shard = winery.base.locked_shard
    content = b"SOMETHING"
    sha256 = objid_for_content(content)["sha256"]
    winery.add(content=content, obj_id=sha256)
    winery.base.set_shard_state(ShardState.FULL)
    winery.base.shard_packing_starts(shard)
    assert pack(shard, **winery.args) is True
    assert winery.get(sha256) == content


@pytest.mark.skipif("CEPH_HARDCODE_POOL" in os.environ, reason="Ceph pool hardcoded")
def test_winery_ceph_pool(needs_ceph, rbd_map_options):
    name = "IMAGE"
    pool = PoolHelper(
        shard_max_size=10 * 1024 * 1024,
        rbd_pool_name="test-winery-ceph-pool",
        rbd_map_options=rbd_map_options,
    )
    pool.remove()
    pool.pool_create()
    assert pool.image_mapped(name) is None
    pool.image_create(name)
    assert pool.image_mapped(name) == "rw"
    p = pool.image_path(name)
    assert p.endswith(name)
    something = "SOMETHING"
    open(p, "w").write(something)
    assert open(p).read(len(something)) == something
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


def test_winery_leaky_bucket_tick(mocker):
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


def test_winery_leaky_bucket_reset():
    b = LeakyBucket(100)
    assert b.total == 100
    assert b.current == b.total
    b.reset(50)
    assert b.total == 50
    assert b.current == b.total
    b.reset(100)
    assert b.total == 100
    assert b.current == 50


def test_winery_bandwidth_calculator(mocker):
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


def test_winery_io_throttler(postgresql_dsn, mocker):
    sleep = mocker.spy(time, "sleep")
    speed = 100
    i = IOThrottler("read", base_dsn=postgresql_dsn, throttle_read=100)
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


def test_winery_throttler(postgresql_dsn):
    t = Throttler(base_dsn=postgresql_dsn, throttle_read=100, throttle_write=100)

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
    def objstorage(self, file_backed_pool, storage):
        self.storage = storage

    @pytest.mark.skip("This interface is not supported as such by winery")
    def test_restore_content(self):
        pass

    @pytest.mark.skip("Winery has more extended signatures than the interface expects")
    def test_types(self):
        pass
