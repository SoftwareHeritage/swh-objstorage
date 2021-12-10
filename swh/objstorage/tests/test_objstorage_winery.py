# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import time

import pytest
import sh

from swh.objstorage import exc
from swh.objstorage.backends.winery.database import Database
from swh.objstorage.backends.winery.objstorage import Packer, pack
from swh.objstorage.factory import get_objstorage

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
        cls="winery", base_dsn=dsn, shard_dsn=dsn, shard_max_size=shard_max_size
    )
    yield storage
    storage.winery.uninit()
    #
    # pytest-postgresql will not remove databases that it did not
    # create between tests (only at the very end).
    #
    d = Database(dsn)
    for database in d.list_databases():
        if database != postgresql.info.dbname and database != "tests_tmpl":
            d.drop_database(database)


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
    obj_id = winery.add(content=content)
    assert obj_id.hex() == "0c8c841f7d9fd4874d841506d3ffc16808b1d579"
    assert winery.add(content=content, obj_id=obj_id) == obj_id
    assert winery.add(content=content, obj_id=obj_id, check_presence=False) == obj_id
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
    obj_id = winery.add(content=content)
    assert obj_id.hex() == "0c8c841f7d9fd4874d841506d3ffc16808b1d579"
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


@pytest.mark.asyncio
async def test_winery_bench_real(pytestconfig, postgresql, ceph_pool):
    dsn = (
        f"postgres://{postgresql.info.user}"
        f":@{postgresql.info.host}:{postgresql.info.port}"
    )
    kwargs = {
        "rw_workers": pytestconfig.getoption("--winery-bench-rw-workers"),
        "ro_workers": pytestconfig.getoption("--winery-bench-ro-workers"),
        "shard_max_size": pytestconfig.getoption("--winery-shard-max-size"),
        "ro_worker_max_request": pytestconfig.getoption(
            "--winery-bench-ro-worker-max-request"
        ),
        "duration": pytestconfig.getoption("--winery-bench-duration"),
        "base_dsn": dsn,
        "shard_dsn": dsn,
    }
    assert await Bench(kwargs).run() == kwargs["rw_workers"] + kwargs["ro_workers"]


@pytest.mark.asyncio
async def test_winery_bench_fake(pytestconfig, mocker):
    kwargs = {
        "rw_workers": pytestconfig.getoption("--winery-bench-rw-workers"),
        "ro_workers": pytestconfig.getoption("--winery-bench-ro-workers"),
        "duration": pytestconfig.getoption("--winery-bench-duration"),
    }

    def run(kind):
        time.sleep(kwargs["duration"] * 2)
        return kind

    mocker.patch("swh.objstorage.tests.winery_benchmark.Worker.run", side_effect=run)
    assert await Bench(kwargs).run() == kwargs["rw_workers"] + kwargs["ro_workers"]
