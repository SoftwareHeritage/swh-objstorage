# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import shutil
import time
from typing import Dict

import pytest

from swh.objstorage import exc
from swh.objstorage.backends.winery.database import DatabaseAdmin
from swh.objstorage.backends.winery.objstorage import pack
from swh.objstorage.backends.winery.sharedbase import ShardState
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

from .winery_benchmark import Bench, ROWorker, RWWorker, WorkerKind, work
from .winery_testing_helpers import PoolHelper, SharedBaseHelper

logger = logging.getLogger(__name__)


@pytest.fixture
def needs_ceph():
    ceph = shutil.which("ceph")

    if not ceph:
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
def postgresql_dsn(postgresql_proc):
    dsn = f"user={postgresql_proc.user} host={postgresql_proc.host} port={postgresql_proc.port}"
    yield dsn
    d = DatabaseAdmin(dsn)
    for database in d.list_databases():
        if database not in (postgresql_proc.dbname, f"{postgresql_proc.dbname}_tmpl"):
            DatabaseAdmin(dsn, database).drop_database()


@pytest.fixture
def storage(request, postgresql_dsn):
    marker = request.node.get_closest_marker("shard_max_size")
    if marker is None:
        shard_max_size = 1024
    else:
        shard_max_size = marker.args[0]
    storage = get_objstorage(
        cls="winery",
        base_dsn=postgresql_dsn,
        shard_dsn=postgresql_dsn,
        shard_max_size=shard_max_size,
        throttle_write=200 * 1024 * 1024,
        throttle_read=100 * 1024 * 1024,
    )
    logger.debug("Instantiated storage %s", storage)
    yield storage
    storage.winery.uninit()


@pytest.fixture
def winery(storage):
    return storage.winery


def test_winery_sharedbase(winery):
    base = winery.base
    shard1 = base.locked_shard
    assert shard1 is not None
    assert shard1 == base.locked_shard

    id1 = base.locked_shard_id
    assert id1 is not None
    assert id1 == base.locked_shard_id

    helper = SharedBaseHelper(winery.base)

    assert helper.get_shard_info_by_name(shard1) is ShardState.WRITING

    winery.base.uninit()

    assert winery.base._locked_shard is None
    assert helper.get_shard_info_by_name(shard1) is ShardState.STANDBY

    shard2 = winery.base.locked_shard

    assert shard1 == shard2, "Locked a different shard?"
    assert helper.get_shard_info_by_name(shard1) is ShardState.WRITING


def test_winery_add_get(winery):
    shard = winery.base.locked_shard
    content = b"SOMETHING"
    obj_id = compute_hash(content, "sha256")
    assert (
        obj_id.hex()
        == "866878b165607851782d8d233edf0c261172ff67926330d3bbd10c705b92d24f"
    )
    winery.add(content=content, obj_id=obj_id)
    winery.add(content=content, obj_id=obj_id)
    winery.add(content=content, obj_id=obj_id, check_presence=False)
    assert winery.base.locked_shard == shard
    assert winery.get(obj_id) == content
    with pytest.raises(exc.ObjNotFoundError):
        winery.get(b"unknown")
    winery.shard.drop()


@pytest.mark.shard_max_size(1)
def test_winery_add_and_pack(winery, mocker):
    mocker.patch("swh.objstorage.backends.winery.objstorage.pack", return_value=True)
    shard = winery.base.locked_shard
    content = b"SOMETHING"
    winery.add(content=content, obj_id=compute_hash(content, "sha256"))
    assert winery.base.locked_shard != shard
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
    shard = winery.base.locked_shard
    content = b"SOMETHING"
    obj_id = compute_hash(content, "sha256")
    winery.add(content=content, obj_id=obj_id)
    winery.base.set_shard_state(ShardState.FULL)
    winery.base.shard_packing_starts(shard)
    assert pack(shard, **winery.args)

    assert (
        SharedBaseHelper(winery.base).get_shard_info_by_name(shard)
        is ShardState.READONLY
    )


@pytest.mark.shard_max_size(10 * 1024 * 1024)
def test_winery_get_object(winery, ceph_pool):
    shard = winery.base.locked_shard
    content = b"SOMETHING"
    obj_id = compute_hash(content, "sha256")
    winery.add(content=content, obj_id=obj_id)
    winery.base.set_shard_state(ShardState.FULL)
    winery.base.shard_packing_starts(shard)
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
def test_winery_bench_work(storage, ceph_pool, tmpdir):
    #
    # rw worker creates a shard
    #
    locked_shard = storage.winery.base.locked_shard
    shards_info = list(storage.winery.base.list_shards())
    assert shards_info == [(locked_shard, ShardState.WRITING)]
    assert work("rw", storage) == "rw"
    shards_info = dict(storage.winery.base.list_shards())
    assert len(shards_info) == 2
    assert shards_info[locked_shard].readonly_available
    #
    # ro worker reads a shard
    #
    args = {**storage.winery.args, "readonly": True}
    assert work("ro", args, {"ro": {"max_request": 1}}) == "ro"


@pytest.mark.shard_max_size(10 * 1024 * 1024)
def test_winery_bench_rw_object_limit(storage):
    object_limit = 15
    worker = RWWorker(
        storage, object_limit=object_limit, single_shard=False, block_until_packed=False
    )

    assert worker.run() == "rw"

    with storage.winery.base.db.cursor() as c:
        c.execute("SELECT count(*) from signature2shard")
        assert c.fetchone() == (object_limit,)


@pytest.mark.shard_max_size(10 * 1024 * 1024)
def test_winery_bench_rw_block_until_packed(storage, ceph_pool):
    worker = RWWorker(storage, single_shard=True, block_until_packed=False)

    assert worker.run() == "rw"

    packed = 0
    for packer in storage.winery.packers:
        packer.join()
        assert packer.exitcode == 0
        packed += 1

    assert packed > 0, "did not have any packers to wait for"


@pytest.mark.shard_max_size(1024 * 1024)
def test_winery_bench_rw_block_until_packed_multiple_shards(storage, ceph_pool):
    # 1000 objects will create multiple shards when the limit is 1MB
    worker = RWWorker(
        storage, object_limit=1000, single_shard=False, block_until_packed=False
    )

    assert worker.run() == "rw"

    packed = 0
    for packer in storage.winery.packers:
        packer.join()
        assert packer.exitcode == 0
        packed += 1

    assert packed > 0, "did not have any packers to wait for"


def test_winery_bench_real(pytestconfig, postgresql_dsn, ceph_pool):
    storage_config = {
        "output_dir": pytestconfig.getoption("--winery-bench-output-directory"),
        "shard_max_size": pytestconfig.getoption("--winery-bench-shard-max-size"),
        "base_dsn": postgresql_dsn,
        "shard_dsn": postgresql_dsn,
        "throttle_read": pytestconfig.getoption("--winery-bench-throttle-read"),
        "throttle_write": pytestconfig.getoption("--winery-bench-throttle-write"),
    }
    workers_per_kind: Dict[WorkerKind, int] = {
        "ro": pytestconfig.getoption("--winery-bench-ro-workers"),
        "rw": pytestconfig.getoption("--winery-bench-rw-workers"),
    }
    worker_args: Dict[WorkerKind, Dict] = {
        "ro": {
            "max_request": pytestconfig.getoption(
                "--winery-bench-ro-worker-max-request"
            )
        }
    }
    duration = pytestconfig.getoption("--winery-bench-duration")
    count = call_async(
        Bench(
            storage_config=storage_config,
            duration=duration,
            workers_per_kind=workers_per_kind,
            worker_args=worker_args,
        ).run
    )
    assert count > 0


def test_winery_bench_fake(pytestconfig, postgresql_dsn, mocker):
    storage_config = {
        "output_dir": pytestconfig.getoption("--winery-bench-output-directory"),
        "shard_max_size": pytestconfig.getoption("--winery-bench-shard-max-size"),
        "base_dsn": postgresql_dsn,
        "shard_dsn": postgresql_dsn,
        "throttle_read": pytestconfig.getoption("--winery-bench-throttle-read"),
        "throttle_write": pytestconfig.getoption("--winery-bench-throttle-write"),
    }
    workers_per_kind: Dict[WorkerKind, int] = {
        "ro": pytestconfig.getoption("--winery-bench-ro-workers"),
        "rw": pytestconfig.getoption("--winery-bench-rw-workers"),
    }
    worker_args: Dict[WorkerKind, Dict] = {
        "ro": {
            "max_request": pytestconfig.getoption(
                "--winery-bench-ro-worker-max-request"
            )
        }
    }
    duration = pytestconfig.getoption("--winery-bench-duration")

    class _ROWorker(ROWorker):
        def run(self):
            logger.info("running ro for %s", duration)
            return "ro"

    class _RWWorker(RWWorker):
        def run(self):
            logger.info("running rw for %s", duration)
            return "rw"

    mocker.patch("swh.objstorage.tests.winery_benchmark.ROWorker", _ROWorker)
    mocker.patch("swh.objstorage.tests.winery_benchmark.RWWorker", _RWWorker)
    mocker.patch(
        "swh.objstorage.tests.winery_benchmark.Bench.timeout", side_effect=lambda: True
    )

    count = call_async(
        Bench(
            storage_config=storage_config,
            duration=duration,
            workers_per_kind=workers_per_kind,
            worker_args=worker_args,
        ).run
    )
    assert count == sum(workers_per_kind.values())


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
    DatabaseAdmin(postgresql_dsn, "throttler").create_database()
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
