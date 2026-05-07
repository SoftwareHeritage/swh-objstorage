# Copyright (C) 2021-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import partial
import logging
import threading

from click.testing import CliRunner
import psycopg
import pytest
from pytest_postgresql import factories

from swh.core.db.db_utils import initialize_database_for_module
from swh.objstorage.backends.winery.objstorage import WineryObjStorage
from swh.objstorage.backends.winery.roshard import FileBackedPool
import swh.objstorage.backends.winery.settings as settings
from swh.objstorage.backends.winery.sharedbase import SharedBase
from swh.objstorage.factory import get_objstorage

logger = logging.getLogger(__name__)


@pytest.fixture
def cli_runner(capsys):
    "Run click commands with log capture disabled"

    class CapsysDisabledCliRunner(CliRunner):
        def invoke(self, *args, **kwargs):
            with capsys.disabled():
                return super().invoke(*args, **kwargs)

    return CapsysDisabledCliRunner()


@pytest.fixture
def pool_name(request, pytestconfig):
    return "winery-test-shards"


@pytest.fixture
def file_backed_pool(mocker, tmp_path, shard_max_size, pool_name):
    pool = FileBackedPool(
        base_directory=tmp_path,
        shard_max_size=10 * 1024 * 1024,
        pool_name=pool_name,
    )
    pool.image_unmap_all()
    mocker.patch(
        "swh.objstorage.backends.winery.roshard.RBDPool.from_kwargs",
        return_value=pool,
    )
    pool._settings_for_tests = {
        "type": "directory",
        "base_directory": str(tmp_path),
        "pool_name": pool_name,
    }
    yield pool


@pytest.fixture
def image_pool(file_backed_pool):
    return file_backed_pool


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
def pack_immediately(request) -> bool:
    marker = request.node.get_closest_marker("pack_immediately")
    if marker is None:
        return True
    else:
        return marker.args[0]


@pytest.fixture
def use_throttler(request) -> int:
    marker = request.node.get_closest_marker("use_throttler")
    if marker is None:
        return True
    else:
        return marker.args[0]


@pytest.fixture
def winery_settings(
    postgresql_dsn,
    shard_max_size,
    pack_immediately,
    image_pool,
    use_throttler,
) -> settings.Winery:
    return dict(
        shards={"max_size": shard_max_size},
        database={"db": postgresql_dsn},
        throttler=(
            {
                "db": postgresql_dsn,
                "max_write_bps": 200 * 1024 * 1024,
                "max_read_bps": 100 * 1024 * 1024,
            }
            if use_throttler
            else None
        ),
        packer={
            "create_images": True,
            "pack_immediately": pack_immediately,
        },
        shards_pool=image_pool._settings_for_tests,
    )


@pytest.fixture
def storage(
    winery_settings,
    pool_name,
):
    storage = get_objstorage(cls="winery", **winery_settings)
    assert isinstance(storage, WineryObjStorage)
    logger.debug("Instantiated storage %s on rbd pool %s", storage, pool_name)
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
    pool_name,
):
    storage = get_objstorage(
        cls="winery",
        readonly=True,
        database={"db": readonly_postgresql_dsn},
        shards_pool=winery_settings["shards_pool"],
        shards=winery_settings["shards"],
        throttler=None,
    )
    yield storage
    storage.on_shutdown()


@pytest.fixture
def winery_reader(storage):
    return storage.reader


@pytest.fixture
def winery_writer(storage):
    return storage.writer
