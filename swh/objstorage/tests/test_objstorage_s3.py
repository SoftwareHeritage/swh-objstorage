# Copyright (C) 2016-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import closing, contextmanager
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time
from typing import Iterator, Optional, cast

import aiohttp
from libcloud.storage.providers import get_driver
from libcloud.storage.types import Provider
from moto.server import ThreadedMotoServer
import pytest

from swh.objstorage.backends.s3 import S3CloudObjStorage
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.factory import get_objstorage
from swh.objstorage.objstorage import objid_for_content

from .objstorage_testing import ObjStorageTestFixture

API_KEY = "THE_API_KEY"
API_SECRET_KEY = "THE_API_SECRET_KEY"
CONTAINER_NAME = "testcontainer"


def _find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


@contextmanager
def delete_objects(config) -> Iterator[None]:
    driver_cls = get_driver(Provider.S3)
    driver = driver_cls(**config)
    container = driver.get_container(container_name=CONTAINER_NAME)

    try:
        yield
    finally:
        for obj in driver.list_container_objects(container):
            driver.delete_object(obj)


@pytest.fixture(scope="class")
def moto_server():
    port = _find_free_port()
    moto_config = {
        "key": API_KEY,
        "secret": API_SECRET_KEY,
        "host": "localhost",
        "port": port,
        "secure": False,
        "container_name": CONTAINER_NAME,
    }
    server = ThreadedMotoServer(port=port)
    server.start()

    driver_cls = get_driver(Provider.S3)
    driver = driver_cls(**moto_config)
    container = driver.create_container(container_name=CONTAINER_NAME)
    try:
        yield moto_config
    finally:
        driver.delete_container(container)
        server.stop()


@pytest.fixture
def moto_container(moto_server):
    config = moto_server

    with delete_objects(config):
        yield config


@pytest.fixture(scope="class")
def garage_server(tmpdir_factory):
    executable = shutil.which("garage")
    if executable is None:
        raise pytest.skip("garage is not available")
    tmpdir = Path(tmpdir_factory.mktemp("garage"))
    port = _find_free_port()
    rpc_port = _find_free_port()
    conf = f"""
metadata_dir = "{tmpdir}/meta"
data_dir = "{tmpdir}/data"
db_engine = "sqlite"

replication_factor = 1

rpc_bind_addr = "[::]:{rpc_port}"
rpc_public_addr = "127.0.0.1:{rpc_port}"
rpc_secret = "8739871279eb387ab2f54f5c99200a59f4886a9fb74b6fb3e5402df10fb0bfa5"

[s3_api]
s3_region = "us-east-1"
api_bind_addr = "[::]:{port}"
root_domain = "localhost"
public_endpoint = "http://localhost:{port}"
"""
    conf_path = tmpdir / "config.toml"
    conf_path.write_text(conf)
    proc = subprocess.Popen(
        [executable, "server", "--single-node", "--default-bucket"],
        env={
            "GARAGE_CONFIG_FILE": conf_path,
            "GARAGE_DEFAULT_ACCESS_KEY": API_KEY,
            "GARAGE_DEFAULT_SECRET_KEY": API_SECRET_KEY,
            "GARAGE_DEFAULT_BUCKET": CONTAINER_NAME,
            **os.environ,
        },
    )
    while True:
        s = socket.socket()
        try:
            s.connect(("localhost", port))
        except ConnectionRefusedError:
            time.sleep(0.01)
        else:
            break
    try:
        yield {
            "key": API_KEY,
            "secret": API_SECRET_KEY,
            "host": "localhost",
            "port": port,
            "secure": False,
            "container_name": CONTAINER_NAME,
        }

    finally:
        proc.kill()


@pytest.fixture
def garage_container(garage_server):
    config = garage_server

    with delete_objects(config):
        yield garage_server


@pytest.fixture(params=["moto", "garage"])
def s3_container(request):
    if request.param == "moto":
        return request.getfixturevalue("moto_container")
    else:
        return request.getfixturevalue("garage_container")


class TestMotoS3ObjStorage(ObjStorageTestFixture):
    compression = "none"
    path_prefix: Optional[str] = None

    @pytest.fixture
    def swh_objstorage_config(self, moto_container):
        yield {
            "cls": "s3",
            "compression": self.compression,
            "path_prefix": self.path_prefix,
            **moto_container,
        }


class TestCompressedS3ObjStorage(TestMotoS3ObjStorage):
    compression = "gzip"


class TestPrefixedS3ObjStorage(TestMotoS3ObjStorage):
    path_prefix = "prefix/"


class TestGarageS3ObjStorage(ObjStorageTestFixture):
    compression = "none"
    path_prefix: Optional[str] = None

    @pytest.fixture
    def swh_objstorage_config(self, garage_container):
        yield {
            "cls": "s3",
            "compression": self.compression,
            **garage_container,
        }


def test_moto_anonymous_reads(moto_container) -> None:
    # Get a read-write objstorage
    config = {"cls": "s3", **moto_container}
    rw_objstorage = cast(S3CloudObjStorage, get_objstorage(**config))

    # Allow anonymous reads
    policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicRead",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{CONTAINER_NAME}/*",
                }
            ],
        }
    ).encode()
    r = rw_objstorage._async_loop.run_until_complete(
        rw_objstorage._request(
            "PUT",
            f"http://{rw_objstorage._host}/{CONTAINER_NAME}?policy=",
            content=policy,
            headers={"content-type": "application/json"},
        )
    )
    assert r.status == 200, rw_objstorage._async_loop.run_until_complete(r.read())
    r = rw_objstorage._async_loop.run_until_complete(
        rw_objstorage._request(
            "PUT",
            f"http://{rw_objstorage._host}/{CONTAINER_NAME}?acl=",
            headers={"x-amz-acl": "public-read"},
        )
    )
    assert r.status == 200, rw_objstorage._async_loop.run_until_complete(r.read())

    # Get a read-only objstorage
    config = {**config, "key": None, "secret": None}
    ro_objstorage = get_objstorage(**config)

    content = b"foo bar"
    obj_id = objid_for_content(content)

    # TODO: test this. we can't test on Moto (it unconditionally accepts) nor on Garage
    # (it does not support bucket policies)
    # with pytest.raises(
    #     Exception, match="Unexpected PUT status code.*403 Forbidden"
    # ):
    #     ro_objstorage.add(content, obj_id)

    # object not present
    assert obj_id not in rw_objstorage
    assert obj_id not in ro_objstorage
    with pytest.raises(ObjNotFoundError):
        ro_objstorage.get(obj_id)
    assert list(ro_objstorage.get_batch([obj_id])) == []

    # add object
    rw_objstorage.add(content, obj_id)
    assert obj_id in rw_objstorage

    # object present
    assert obj_id in ro_objstorage
    assert ro_objstorage.get(obj_id) == content
    assert list(ro_objstorage.get_batch([obj_id])) == [(obj_id, content)]


def test_s3_retries(mocker, moto_container) -> None:
    # Get a read-write objstorage
    config = {"cls": "s3", **moto_container}
    objstorage = cast(S3CloudObjStorage, get_objstorage(**config))

    real_request = objstorage._session.request
    i = 0

    def crashy_request(*args, **kwargs):
        nonlocal i
        i += 1
        if i % 3 == 0:
            return real_request(*args, **kwargs)
        elif i % 3 == 1:
            raise aiohttp.ServerDisconnectedError("Mock error")
        else:
            raise aiohttp.ClientPayloadError("Mock error")

    mock = mocker.patch.object(
        objstorage._session, "request", side_effect=crashy_request
    )

    content = b"foo bar"
    obj_id = objid_for_content(content)

    # object not present (3 calls)
    assert obj_id not in objstorage
    with pytest.raises(ObjNotFoundError):
        objstorage.get(obj_id)
    assert list(objstorage.get_batch([obj_id])) == []

    # add object (2 calls: 1 HEAD and 1 PUT)
    objstorage.add(content, obj_id)

    # object present (3 calls)
    assert obj_id in objstorage
    assert objstorage.get(obj_id) == content
    assert list(objstorage.get_batch([obj_id])) == [(obj_id, content)]

    assert mock.call_count == 8 * 3  # seven calls, each fails twice
