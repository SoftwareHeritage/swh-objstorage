# Copyright (C) 2021-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import partial
from itertools import cycle
import logging
import os
import shutil
import tempfile
import threading
from typing import Dict, Generator, List
import uuid

from click.testing import CliRunner
import psycopg
import pytest
from pytest_postgresql import factories

from swh.core.db.db_utils import initialize_database_for_module
from swh.objstorage.backends.winery.housekeeping import import_ro_shards
from swh.objstorage.backends.winery.objstorage import WineryObjStorage
from swh.objstorage.backends.winery.pools.rbd import RBDPool
from swh.objstorage.backends.winery.pools.shard import ShardBackedPool
import swh.objstorage.backends.winery.settings as settings
from swh.objstorage.backends.winery.sharedbase import SharedBase
from swh.objstorage.factory import get_objstorage
from swh.objstorage.interface import HashDict
from swh.objstorage.objstorage import objid_for_content
from swh.shard import Shard, ShardCreator

from .winery_testing_helpers import RBDPoolHelper

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
def shards() -> Generator[Dict[str, List[HashDict]], None, None]:
    """A simple fixture generating (legacy) shard files

    Generates a 6 shard files (swh-shard) each with 12 content objects.

    The result is a dict which keys are the shard file path, and values are
    object ids stored in that shard file.

    Note: this is a session scoped fixture to prevent slowing tests down too
    much.

    """
    count = 12
    nshards = 6
    shards: Dict[str, List[HashDict]] = {}
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
            # enforce file to be RO (so it will be considered as mapped by FileBackedPool)
            os.chmod(path, 0o400)

        for path in shards:
            shard = Shard(path)
            assert shard.header.objects_count == count

        yield shards


@pytest.fixture
def needs_ceph(pool_names):
    """Skip the test is ceph is not available but is needed for the test

    aka if there is at least one rbd pool in the pool names
    """

    if any(pool_name.endswith("-rbd") for pool_name in pool_names):
        ceph = shutil.which("ceph")

        if not ceph:
            pytest.skip("the ceph CLI was not found")
        if os.environ.get("USE_CEPH", "no") != "yes":
            pytest.skip(
                "the ceph-based tests have been disabled (USE_CEPH env var is not 'yes')"
            )


@pytest.fixture
def image_pools(tmp_path, shard_max_size, pool_names, needs_ceph):
    """Fixture that generates winery shards pools

    For each pool name in 'pool_names', it will instantiate the corresponding
    Pool backend based on a simple pool name pattern: if the pool name ends with:

    - '-directory': produces a ShardBackedPool
    - '-rbd': produces a RBDPool (actually a RBDPoolHelper, see winery_testing_helpers)

    On teardown, clean RBD pools if needed.

    Note that the 'needs_ceph' fixture will skip any test using this fixture if
    at least one of the pools is expected to be an RBDPool (aka there is a
    least one 'xxx-rbd' pool name in 'pool_names') and the environment is not
    set up to run ceph based tests.

    """
    rbd_map_options = os.environ.get("RBD_MAP_OPTIONS", "")
    rbd_hardcoded_pool = bool(os.environ.get("CEPH_HARDCODE_POOL"))

    pools = []
    for pool_name in pool_names:
        if pool_name.endswith("-directory"):
            pool = ShardBackedPool(
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
        elif pool_name.endswith("-rbd"):
            pool = RBDPoolHelper(
                shard_max_size=10 * 1024 * 1024,
                rbd_pool_name=pool_name,
                rbd_map_options=rbd_map_options,
            )
            if not rbd_hardcoded_pool:
                pool.remove()
                pool.pool_create()
            else:
                logger.info("Not removing pool")

            pool._settings_for_tests = {
                "type": "rbd",
                "pool_name": pool_name,
                "map_options": rbd_map_options,
                "readonly": False,
            }
        pools.append(pool)

    yield pools

    for pool in pools:
        if isinstance(pool, RBDPoolHelper):
            if rbd_hardcoded_pool:
                logger.info("Not removing pool")
            else:
                pool.remove()


@pytest.fixture
def write_pool_name(pool_names) -> str | None:
    """Return the active pool from the list of pool names

    This default implementation select the only '-active-' pool name in the
    list of pool names.

    Checks there is only one active.

    """
    active_pools = [pool_name for pool_name in pool_names if "-active-" in pool_name]
    assert len(active_pools) <= 1, "There can be at most one active pool"
    if active_pools:
        return active_pools[0]
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
        readers_cache_size=3,
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
    # NOTE: image_pools != storage.pools so we must unmap those in storage to avoid
    # lingering RBD mounts
    for pool in storage.pools.values():
        if isinstance(pool, RBDPool):
            for image in pool.image_list():
                pool.image_unmap(image)
    names = [
        thread.name
        for thread in threading.enumerate()
        if thread.name.startswith("IdleHandler")
    ]
    assert not names, f"Some IdleHandlers are still alive: {','.join(names)}"


@pytest.fixture
def prefilled_storage(storage, shards, image_pools):
    """
    Same as storage, but all pools are pre-filled with shards' contents using only
    Pool's API and direct file access, to ensure Ceph compatibility.

    Those shards will be removed properly by the `image_pools` fixture.
    """
    assert isinstance(storage, WineryObjStorage)

    for pool, shard_path in zip(cycle(image_pools), shards.keys()):
        pool.image_import(shard_path)
    n_objs = 0
    n_shards = 0
    for pool in storage.pools.values():
        o, s = import_ro_shards(storage.writer.base, pool)
        n_objs += o
        n_shards += s
    assert n_shards == 6
    assert n_objs == 12 * 6
    yield storage


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
