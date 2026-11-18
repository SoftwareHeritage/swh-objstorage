# Copyright (C) 2016-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import closing, contextmanager
import socket
from typing import Iterator, Optional

from libcloud.storage.providers import get_driver
from libcloud.storage.types import Provider
from moto.server import ThreadedMotoServer
import pytest

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
