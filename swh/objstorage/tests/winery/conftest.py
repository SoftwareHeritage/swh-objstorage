# Copyright (C) 2021-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import partial
import logging
import os
import tempfile
import threading
import uuid

from click.testing import CliRunner
import psycopg
import pytest
from pytest_postgresql import factories

from swh.core.db.db_utils import initialize_database_for_module
from swh.objstorage.backends.winery.objstorage import WineryObjStorage
from swh.objstorage.backends.winery.pools import FileBackedPool
import swh.objstorage.backends.winery.settings as settings
from swh.objstorage.backends.winery.sharedbase import SharedBase
from swh.objstorage.factory import get_objstorage
from swh.objstorage.objstorage import objid_for_content
from swh.shard import Shard, ShardCreator

logger = logging.getLogger(__name__)


@pytest.fixture
def cli_runner(capsys):
    "Run click commands with log capture disabled"

    class CapsysDisabledCliRunner(CliRunner):
        def invoke(self, *args, **kwargs):
            with capsys.disabled():
                return super().invoke(*args, **kwargs)

    return CapsysDisabledCliRunner()


@pytest.fixture(scope="session")
def shards():
    count = 12
    nshards = 6
    shards = {}
    with tempfile.TemporaryDirectory() as shards_dir:
        for nshard in range(nshards):
            name = "i" + uuid.uuid4().hex[1:]
            path = os.path.join(shards_dir, name)
            shards[path] = []
            with ShardCreator(path, count) as shard:
                for i in range(count):
                    content = b"Housekeeping shard:%d content:%d" % (nshard, i)
                    objid = objid_for_content(content)
                    shard.write(objid["sha256"], content)
                    shards[path].append(objid)
        for path in shards:
            shard = Shard(path)
            assert shard.header.objects_count == count
        yield shards


@pytest.fixture
def pool_names(request, pytestconfig):
    return ["winery-test-shards"]


@pytest.fixture
def file_backed_pools(tmp_path, shard_max_size, pool_names):
    pools = []
    for pool_name in pool_names:
        pool = FileBackedPool(
            base_directory=tmp_path,
            shard_max_size=shard_max_size,
            pool_name=pool_name,
        )
        pool.image_unmap_all()
        pool._settings_for_tests = {
            "type": "directory",
            "base_directory": str(tmp_path),
            "pool_name": pool_name,
        }
        pools.append(pool)
    return pools


@pytest.fixture
def image_pools(file_backed_pools):
    return file_backed_pools


@pytest.fixture
def write_pool_name(image_pools):
    rw_pools = [pool for pool in image_pools if "-ro" not in pool.pool_name]
    assert len(rw_pools) <= 1
    if rw_pools:
        return rw_pools[0].pool_name
    return None


def add_guest_user(**kwargs):
    with psycopg.connect(**kwargs) as conn:
        conn.execute("CREATE USER guest WITH PASSWORD 'guest'")
        conn.execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO guest"
        )
        conn.execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO guest"
        )


winery_postgresql_proc = factories.postgresql_proc(
    load=[
        add_guest_user,
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
def readonly_postgresql_dsn(winery_postgresql):
    return (
        f"user=guest password=guest host={winery_postgresql.info.host}"
        f" port={winery_postgresql.info.port} dbname={winery_postgresql.info.dbname}"
    )


@pytest.fixture
def shard_max_size(request) -> int:
    marker = request.node.get_closest_marker("shard_max_size")
    if marker is None:
        return 1024
    else:
        return marker.args[0]


@pytest.fixture
def winery_settings(
    postgresql_dsn,
    shard_max_size,
    image_pools,
    write_pool_name,
) -> settings.Winery:
    return dict(
        shards={"max_size": shard_max_size},
        database={"db": postgresql_dsn},
        packer={
            "create_images": True,
        },
        shards_pools=[pool._settings_for_tests for pool in image_pools],
        shards_active_pool=write_pool_name,
    )


@pytest.fixture
def storage(
    winery_settings,
    image_pools,
):
    storage = get_objstorage(cls="winery", **winery_settings)
    assert isinstance(storage, WineryObjStorage)
    logger.debug(
        "Instantiated storage %s using %s pools (%s)",
        storage,
        len(image_pools),
        ", ".join(
            f"{pool.__class__.__name__}:{pool.pool_name}" for pool in image_pools
        ),
    )
    yield storage
    storage.on_shutdown()
    names = [
        thread.name
        for thread in threading.enumerate()
        if thread.name.startswith("IdleHandler")
    ]
    assert not names, f"Some IdleHandlers are still alive: {','.join(names)}"


@pytest.fixture
def readonly_storage(
    winery_settings,
    readonly_postgresql_dsn,
):
    storage = get_objstorage(
        cls="winery",
        readonly=True,
        database={"db": readonly_postgresql_dsn},
        shards_pools=winery_settings["shards_pools"],
        shards_active_pool=None,
        shards=winery_settings["shards"],
    )
    yield storage
    storage.on_shutdown()


@pytest.fixture
def winery_reader(storage):
    return storage.reader


@pytest.fixture
def winery_writer(storage):
    return storage.writer
