# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import time

import pytest
import sh

from swh.objstorage import exc
from swh.objstorage.backends.winery.database import DatabaseAdmin
from swh.objstorage.backends.winery.objstorage import Packer, pack
from swh.objstorage.backends.winery.stats import Stats
from swh.objstorage.backends.winery.throttler import (
    BandwidthCalculator,
    IOThrottler,
    LeakyBucket,
    Throttler,
)
from swh.objstorage.factory import get_objstorage
from swh.objstorage.objstorage import compute_hash
from swh.objstorage.utils import call_async

from .winery_benchmark import Bench, work
from .winery_testing_helpers import PoolHelper, SharedBaseHelper


@pytest.fixture
def needs_ceph():
    try:
        sh.ceph("--version")
    except sh.CommandNotFound:
        pytest.skip("the ceph CLI was not found")


@pytest.fixture
def ceph_pool(needs_ceph):
    pool = PoolHelper(shard_max_size=10 * 1024 * 1024)
    pool.clobber()
    pool.pool_create()

    yield pool

    pool.images_clobber()
    pool.clobber()


@pytest.fixture
def storage(request, postgresql):
    marker = request.node.get_closest_marker("shard_max_size")
    if marker is None:
        shard_max_size = 1024
    else:
        shard_max_size = marker.args[0]
    dsn = (
        f"postgres://{postgresql.info.user}"
        f":@{postgresql.info.host}:{postgresql.info.port}"
    )
    storage = get_objstorage(
        cls="winery",
        base_dsn=dsn,
        shard_dsn=dsn,
        shard_max_size=shard_max_size,
        throttle_write=200 * 1024 * 1024,
        throttle_read=100 * 1024 * 1024,
    )
    yield storage
    storage.winery.uninit()
    #
    # pytest-postgresql will not remove databases that it did not
    # create between tests (only at the very end).
    #
    d = DatabaseAdmin(dsn)
    for database in d.list_databases():
        if database != postgresql.info.dbname and database != "tests_tmpl":
            DatabaseAdmin(dsn, database).drop_database()


@pytest.fixture
def winery(storage):
    return storage.winery


def test_winery_sharedbase(winery):
    base = winery.base
    shard1 = base.whoami
    assert shard1 is not None
    assert shard1 == base.whoami

    id1 = base.id
    assert id1 is not None
    assert id1 == base.id


def test_winery_add_get(winery):
    shard = winery.base.whoami
    content = b"SOMETHING"
    obj_id = compute_hash(content, "sha256")
    assert (
        obj_id.hex()
        == "866878b165607851782d8d233edf0c261172ff67926330d3bbd10c705b92d24f"
    )
    winery.add(content=content, obj_id=obj_id)
    winery.add(content=content, obj_id=obj_id)
    winery.add(content=content, obj_id=obj_id, check_presence=False)
    assert winery.base.whoami == shard
    assert winery.get(obj_id) == content
    with pytest.raises(exc.ObjNotFoundError):
        winery.get(b"unknown")
    winery.shard.drop()


@pytest.mark.shard_max_size(1)
def test_winery_add_and_pack(winery, mocker):
    mocker.patch("swh.objstorage.backends.winery.objstorage.pack", return_value=True)
    shard = winery.base.whoami
    content = b"SOMETHING"
    winery.add(content=content, obj_id=compute_hash(content, "sha256"))
    assert winery.base.whoami != shard
    assert len(winery.packers) == 1
    packer = winery.packers[0]
    packer.join()
    assert packer.exitcode == 0


def test_winery_delete(storage):
    with pytest.raises(PermissionError):
        storage.delete(None)


def test_winery_get_shard_info(winery):
    assert winery.base.get_shard_info(1234) is None
    assert SharedBaseHelper(winery.base).get_shard_info_by_name("nothing") is None


@pytest.mark.shard_max_size(10 * 1024 * 1024)
def test_winery_packer(winery, ceph_pool):
    shard = winery.base.whoami
    content = b"SOMETHING"
    winery.add(content=content)
    winery.base.shard_packing_starts()
    packer = Packer(shard, **winery.args)
    try:
        assert packer.run() is True
    finally:
        packer.uninit()

    readonly, packing = SharedBaseHelper(winery.base).get_shard_info_by_name(shard)
    assert readonly is True
    assert packing is False


@pytest.mark.shard_max_size(10 * 1024 * 1024)
def test_winery_get_object(winery, ceph_pool):
    shard = winery.base.whoami
    content = b"SOMETHING"
    obj_id = winery.add(content=content)
    winery.base.shard_packing_starts()
    assert pack(shard, **winery.args) is True
    assert winery.get(obj_id) == content


def test_winery_ceph_pool(needs_ceph):
    name = "IMAGE"
    pool = PoolHelper(shard_max_size=10 * 1024 * 1024)
    pool.clobber()
    pool.pool_create()
    pool.image_create(name)
    p = pool.image_path(name)
    assert p.endswith(name)
    something = "SOMETHING"
    open(p, "w").write(something)
    assert open(p).read(len(something)) == something
    assert pool.image_list() == [name]
    pool.image_remap_ro(name)
    pool.images_clobber()
    assert pool.image_list() == [name]
    pool.clobber()
    assert pool.image_list() == []


@pytest.mark.shard_max_size(10 * 1024 * 1024)
def test_winery_bench_work(winery, ceph_pool, tmpdir):
    #
    # rw worker creates a shard
    #
    whoami = winery.base.whoami
    shards_info = list(winery.base.list_shards())
    assert len(shards_info) == 1
    shard, readonly, packing = shards_info[0]
    assert (readonly, packing) == (False, False)
    winery.args["dir"] = str(tmpdir)
    assert work("rw", winery.args) == "rw"
    shards_info = {
        name: (readonly, packing)
        for name, readonly, packing in winery.base.list_shards()
    }
    assert len(shards_info) == 2
    assert shards_info[whoami] == (True, False)
    #
    # ro worker reads a shard
    #
    winery.args["ro_worker_max_request"] = 1
    assert work("ro", winery.args) == "ro"


def test_winery_bench_real(pytestconfig, postgresql, ceph_pool):
    dsn = (
        f"postgres://{postgresql.info.user}"
        f":@{postgresql.info.host}:{postgresql.info.port}"
    )
    kwargs = {
        "output_dir": pytestconfig.getoption("--winery-bench-output-directory"),
        "rw_workers": pytestconfig.getoption("--winery-bench-rw-workers"),
        "ro_workers": pytestconfig.getoption("--winery-bench-ro-workers"),
        "shard_max_size": pytestconfig.getoption("--winery-shard-max-size"),
        "ro_worker_max_request": pytestconfig.getoption(
            "--winery-bench-ro-worker-max-request"
        ),
        "duration": pytestconfig.getoption("--winery-bench-duration"),
        "base_dsn": dsn,
        "shard_dsn": dsn,
        "throttle_read": pytestconfig.getoption("--winery-bench-throttle-read"),
        "throttle_write": pytestconfig.getoption("--winery-bench-throttle-write"),
    }
    count = call_async(Bench(kwargs).run)
    assert count > 0


def test_winery_bench_fake(pytestconfig, mocker):
    kwargs = {
        "rw_workers": pytestconfig.getoption("--winery-bench-rw-workers"),
        "ro_workers": pytestconfig.getoption("--winery-bench-ro-workers"),
        "duration": pytestconfig.getoption("--winery-bench-duration"),
    }

    def run(kind):
        time.sleep(kwargs["duration"] * 2)
        return kind

    mocker.patch("swh.objstorage.tests.winery_benchmark.Worker.run", side_effect=run)
    assert call_async(Bench(kwargs).run) == kwargs["rw_workers"] + kwargs["ro_workers"]


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


def test_winery_io_throttler(postgresql, mocker):
    dsn = (
        f"postgres://{postgresql.info.user}"
        f":@{postgresql.info.host}:{postgresql.info.port}"
    )
    DatabaseAdmin(dsn, "throttler").create_database()
    sleep = mocker.spy(time, "sleep")
    speed = 100
    i = IOThrottler("read", base_dsn=dsn, throttle_read=100)
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


def test_winery_throttler(postgresql):
    dsn = (
        f"postgres://{postgresql.info.user}"
        f":@{postgresql.info.host}:{postgresql.info.port}"
    )
    t = Throttler(base_dsn=dsn, throttle_read=100, throttle_write=100)

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


def test_winery_stats(tmpdir):
    s = Stats(None)
    assert s.stats_active is False
    s = Stats(tmpdir / "stats")
    assert s.stats_active is True
    assert os.path.exists(s.stats_filename)
    size = os.path.getsize(s.stats_filename)
    s._stats_flush_interval = 0
    k = "KEY"
    v = "CONTENT"
    s.stats_read(k, v)
    s.stats_write(k, v)
    s.stats_read(k, v)
    s.stats_write(k, v)
    s.__del__()
    assert os.path.getsize(s.stats_filename) > size
