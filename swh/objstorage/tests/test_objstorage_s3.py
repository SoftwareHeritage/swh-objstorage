# Copyright (C) 2016-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import closing
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time
from typing import Optional

from aioaws.s3 import S3Client, S3Config
from httpx2 import AsyncClient
from libcloud.storage.providers import get_driver
from libcloud.storage.types import Provider
from moto.server import ThreadedMotoServer
import pytest

from swh.objstorage.utils import AsyncLoop

from .objstorage_testing import ObjStorageTestFixture

API_KEY = "THE_API_KEY"
API_SECRET_KEY = "THE_API_SECRET_KEY"
CONTAINER_NAME = "testcontainer"


def _find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


@pytest.fixture(scope="class")
def moto_server():
    port = _find_free_port()
    moto_config = {
        "key": API_KEY,
        "secret": API_SECRET_KEY,
        "host": "localhost",
        "port": port,
        "secure": False,
        "workaround_moto": True,
    }
    server = ThreadedMotoServer(port=port)
    server.start()

    driver_cls = get_driver(Provider.S3)
    driver = driver_cls(**moto_config)
    container = driver.create_container(container_name=CONTAINER_NAME)
    try:
        yield {
            **moto_config,
            "region": "us-east-1",
        }, driver, container
    finally:
        for obj in driver.list_container_objects(container):
            driver.delete_object(obj)
        driver.delete_container(container)
        server.stop()


@pytest.fixture
def moto_container(moto_server):
    moto_config, driver, container = moto_server

    for obj in driver.list_container_objects(container):
        driver.delete_object(obj)

    yield moto_config


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
        }

    finally:
        proc.kill()


@pytest.fixture
def garage_container(garage_server):
    async def clean_bucket() -> None:
        async with AsyncClient() as http_client:
            config = S3Config(
                garage_server["key"],
                garage_server["secret"],
                "us-east-1",
                CONTAINER_NAME,
                f"{garage_server['host']}:{garage_server['port']}/{CONTAINER_NAME}",
            )
            s3_client = S3Client(http_client, config)
            s3_client._aws_client.schema = "http"

            async for obj in s3_client.list():
                await s3_client.delete(obj)

    async_loop = AsyncLoop()
    async_loop.run_until_complete(clean_bucket())

    yield garage_server


@pytest.fixture(params=["moto", "garage"])
def s3_container(request):
    if request.param == "moto":
        return request.getfixturevalue("moto_container")
    else:
        return request.getfixturevalue("garage_container")


class TestCloudObjStorage(ObjStorageTestFixture):
    compression = "none"
    path_prefix: Optional[str] = None

    @pytest.fixture
    def swh_objstorage_config(self, s3_container):
        yield {
            "cls": "s3",
            "container_name": CONTAINER_NAME,
            "compression": self.compression,
            **s3_container,
        }
