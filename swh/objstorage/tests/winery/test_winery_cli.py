# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import datetime
import logging
import os
import re
import tempfile

from click.testing import CliRunner
import pytest
import yaml

from swh.objstorage.backends.winery.cli import objstorage_cli_group
from swh.objstorage.objstorage import objid_for_content

logger = logging.getLogger(__name__)


def now():
    return datetime.datetime.now(tz=datetime.UTC)


def invoke(*args, env=None, config={}, **kwargs):
    config = copy.deepcopy(config)
    config["cls"] = "winery"
    config.update(**kwargs)

    runner = CliRunner()
    with tempfile.NamedTemporaryFile("a", suffix=".yml") as config_fd:
        yaml.dump({"objstorage": config}, config_fd)
        config_fd.seek(0)
        args = ["-C" + config_fd.name] + list(args)
        return runner.invoke(
            objstorage_cli_group,
            args,
            obj={"log_level": logging.DEBUG},
            env=env,
        )


def test_winery_help(winery_settings):
    result = invoke("winery", "--help", config=winery_settings)
    expected = (
        r"^\s*Usage: objstorage winery \[OPTIONS\] COMMAND \[ARGS\]...\s+"
        r"Winery related commands.*"
    )
    assert result.exit_code == 0, result.output
    assert re.match(expected, result.output, re.MULTILINE), result.output


def test_winery_list_open_shards(winery_settings, storage):
    result = invoke("winery", "list-open-shards", config=winery_settings)
    expected = r"^\s*No open shard\s+"
    assert result.exit_code == 0, (result.output, result.stderr, result.exception)
    assert re.match(expected, result.output, re.MULTILINE), result.output

    storage.add(b"toto", objid_for_content(b"toto"))
    storage.add(b"toto2", objid_for_content(b"toto2"))
    result = invoke("winery", "list-open-shards", config=winery_settings)
    expected = (
        r"^\s*Open shards:\s+"
        r"[0-9a-f-]{36}:\s+"
        r"i[0-9a-f]{31}: +WRITING since a moment\s+"
    )
    assert result.exit_code == 0, (result.output, result.stderr, result.exception)
    assert re.match(expected, result.output, re.MULTILINE)

    result = invoke(
        "winery", "list-open-shards", "--state", "writing", config=winery_settings
    )
    expected = (
        r"^\s*Open shards:\s+"
        r"[0-9a-f-]{36}:\s+"
        r"i[0-9a-f]{31}: +WRITING since a moment\s+"
    )
    assert result.exit_code == 0, (result.output, result.stderr, result.exception)
    assert re.match(expected, result.output, re.MULTILINE)

    result = invoke(
        "winery", "list-open-shards", "--state", "full", config=winery_settings
    )
    expected = r"^\s*No shard in the state 'full'\s+"
    assert result.exit_code == 0, (result.output, result.stderr, result.exception)
    assert re.match(expected, result.output, re.MULTILINE)


def test_winery_list_stale_shards_none(winery_settings, storage):
    storage.add(b"toto", objid_for_content(b"toto"))
    storage.add(b"toto2", objid_for_content(b"toto2"))
    result = invoke("winery", "list-stale-shards", config=winery_settings)
    expected = r"^\s*No identified stale shard\s+"
    assert result.exit_code == 0, (result.output, result.stderr, result.exception)
    assert re.match(expected, result.output, re.MULTILINE), result.output


def test_winery_list_stale_shards_some(winery_settings, storage, winery_postgresql):
    storage.add(b"toto", objid_for_content(b"toto"))
    storage.add(b"toto2", objid_for_content(b"toto2"))

    locker_ts = now() - datetime.timedelta(days=7)
    with winery_postgresql.cursor() as cur:
        cur.execute("UPDATE shards SET locker_ts=%s", (locker_ts,))
    winery_postgresql.commit()

    result = invoke(
        "winery", "list-stale-shards", "--duration", "10d", config=winery_settings
    )
    expected = r"^\s*No identified stale shard\s+"
    assert result.exit_code == 0, (result.output, result.stderr, result.exception)
    assert re.match(expected, result.output, re.MULTILINE), result.output

    # shards = storage.
    result = invoke("winery", "list-stale-shards", config=winery_settings)
    expected = (
        r"^\s*Potentially stale shards:\s+"
        r"[0-9a-f-]{36}:\s+"
        r"i[0-9a-f]{31}: +WRITING since 7 days\s+"
    )
    assert result.exit_code == 0, (result.output, result.stderr, result.exception)
    assert re.match(expected, result.output, re.MULTILINE), result.output


@pytest.mark.parametrize(
    "from_state, to_state",
    (("writing", "standby"), ("packing", "full"), ("cleaning", "packed")),
)
def test_winery_release_stale_shards(
    winery_settings, storage, winery_postgresql, from_state, to_state
):
    storage.add(b"toto", objid_for_content(b"toto"))
    storage.add(b"toto2", objid_for_content(b"toto2"))

    locker_ts = now() - datetime.timedelta(days=7)
    with winery_postgresql.cursor() as cur:
        cur.execute(
            "UPDATE shards SET state=%s, locker_ts=%s",
            (
                from_state,
                locker_ts,
            ),
        )
    winery_postgresql.commit()

    result = invoke(
        "winery", "release-stale-shards", "--dry-run", config=winery_settings
    )
    expected = (
        r"^\s*Would release \(dry run\):\s+"
        r"[0-9a-f-]{36}:\s+"
        r"i[0-9a-f]{31} stuck in "
        rf"{from_state.upper()} for 7 days --> {to_state.upper()}\s+"
    )
    assert result.exit_code == 0, (result.output, result.stderr, result.exception)
    assert re.match(expected, result.output, re.MULTILINE), result.output

    result = invoke("winery", "list-stale-shards", config=winery_settings)
    expected = (
        r"^\s*Potentially stale shards:\s+"
        r"[0-9a-f-]{36}:\s+"
        r"i[0-9a-f]{31}: +"
        rf"{from_state.upper()} since 7 days\s+"
    )
    assert result.exit_code == 0, (result.output, result.stderr, result.exception)
    assert re.match(expected, result.output, re.MULTILINE), result.output

    result = invoke("winery", "release-stale-shards", config=winery_settings)
    expected = (
        r"^\s*Releasing:\s+"
        r"[0-9a-f-]{36}:\s+"
        r"i[0-9a-f]{31} stuck in "
        rf"{from_state.upper()} for 7 days --> {to_state.upper()}\s+"
    )
    assert result.exit_code == 0, (result.output, result.stderr, result.exception)
    assert re.match(expected, result.output, re.MULTILINE), result.output

    result = invoke("winery", "list-stale-shards", config=winery_settings)
    expected = r"^\s*No identified stale shard\s+"
    assert result.exit_code == 0, (result.output, result.stderr, result.exception)
    assert re.match(expected, result.output, re.MULTILINE), result.output


def test_winery_import_shards_nothing(winery_settings):
    result = invoke("winery", "import-shards", config=winery_settings)
    assert result.exit_code == 0
    assert "Pool winery-test-shards: nothing to do" in result.stdout


def test_winery_import_shards_do_import(storage, winery_settings, shards):
    pool = storage.pool
    pooldir = pool.base_directory / pool.pool_name
    for shard in shards:
        name = os.path.basename(shard)
        os.link(shard, pooldir / name)

    result = invoke("winery", "import-shards", config=winery_settings)
    assert result.exit_code == 0
    assert "Pool winery-test-shards: imported 72 objects from 6 shards" in result.stdout
