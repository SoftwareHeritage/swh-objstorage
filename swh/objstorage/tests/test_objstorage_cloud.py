# Copyright (C) 2016-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import closing
import secrets
import socket
from typing import Optional
import unittest

from libcloud.common.types import InvalidCredsError
from libcloud.storage.providers import get_driver
from libcloud.storage.types import (
    ContainerDoesNotExistError,
    ObjectDoesNotExistError,
    Provider,
)
import pytest

from swh.objstorage.backends.libcloud import CloudObjStorage
from swh.objstorage.exc import Error
from swh.objstorage.factory import get_objstorage
from swh.objstorage.objstorage import decompressors

from .objstorage_testing import ObjStorageTestFixture

API_KEY = "API_KEY"
API_SECRET_KEY = "API SECRET KEY"
CONTAINER_NAME = "test_container"


class MockLibcloudObject:
    """Libcloud object mock that replicates its API"""

    def __init__(self, name, content):
        self.name = name
        self.content = list(content)

    def as_stream(self):
        yield from iter(self.content)


class MockLibcloudDriver:
    """Mock driver that replicates the used LibCloud API"""

    def __init__(self, api_key, api_secret_key):
        self.containers = {CONTAINER_NAME: {}}  # Storage is initialized
        self.api_key = api_key
        self.api_secret_key = api_secret_key

    def _check_credentials(self):
        # Private method may be known as another name in Libcloud but is used
        # to replicate libcloud behavior (i.e. check credential at each
        # request)
        if self.api_key != API_KEY or self.api_secret_key != API_SECRET_KEY:
            raise InvalidCredsError()

    def get_container(self, container_name):
        try:
            return self.containers[container_name]
        except KeyError:
            raise ContainerDoesNotExistError(
                container_name=container_name, driver=self, value=None
            )

    def iterate_container_objects(self, container):
        self._check_credentials()
        yield from (v for k, v in sorted(container.items()))

    def get_object(self, container_name, obj_id):
        self._check_credentials()
        try:
            container = self.get_container(container_name)
            return container[obj_id]
        except KeyError:
            raise ObjectDoesNotExistError(object_name=obj_id, driver=self, value=None)

    def delete_object(self, obj):
        self._check_credentials()
        try:
            container = self.get_container(CONTAINER_NAME)
            container.pop(obj.name)
            return True
        except KeyError:
            raise ObjectDoesNotExistError(object_name=obj.name, driver=self, value=None)

    def upload_object_via_stream(self, content, container, obj_id):
        self._check_credentials()
        obj = MockLibcloudObject(obj_id, content)
        container[obj_id] = obj


class MockCloudObjStorage(CloudObjStorage):
    """Cloud object storage that uses a mocked driver"""

    def _get_driver(self, **kwargs):
        return MockLibcloudDriver(**kwargs)

    def _get_provider(self):
        # Implement this for the abc requirement, but behavior is defined in
        # _get_driver.
        pass


class TestCloudObjStorage(ObjStorageTestFixture, unittest.TestCase):
    compression = "none"
    path_prefix: Optional[str] = None

    def setUp(self):
        super().setUp()
        self.storage = MockCloudObjStorage(
            CONTAINER_NAME,
            api_key=API_KEY,
            api_secret_key=API_SECRET_KEY,
            compression=self.compression,
            path_prefix=self.path_prefix,
        )

    def test_compression(self):
        content, obj_id = self.hash_content(b"add_get_w_id")
        self.storage.add(content, obj_id=obj_id)

        libcloud_object = self.storage._get_object(obj_id)
        raw_content = b"".join(libcloud_object.content)

        d = decompressors[self.compression]()
        assert d.decompress(raw_content) == content
        assert d.unused_data == b""

    def test_trailing_data_on_stored_blob(self):
        content, obj_id = self.hash_content(b"test content without garbage")
        self.storage.add(content, obj_id=obj_id)

        libcloud_object = self.storage._get_object(obj_id)
        libcloud_object.content.append(b"trailing garbage")

        if self.compression == "none":
            with self.assertRaises(Error) as e:
                self.storage.check(obj_id)
        else:
            with self.assertRaises(Error) as e:
                self.storage.get(obj_id)
            assert "trailing data" in e.exception.args[0]


class TestCloudObjStorageBz2(TestCloudObjStorage):
    compression = "bz2"


class TestCloudObjStorageGzip(TestCloudObjStorage):
    compression = "gzip"


class TestCloudObjStorageLzma(TestCloudObjStorage):
    compression = "lzma"


class TestCloudObjStorageZlib(TestCloudObjStorage):
    compression = "zlib"


class TestCloudObjStoragePrefix(TestCloudObjStorage):
    path_prefix = "contents"

    def test_path_prefix(self):
        content, obj_id = self.hash_content(b"test content")
        self.storage.add(content, obj_id=obj_id)

        container = self.storage.driver.containers[CONTAINER_NAME]
        object_path = self.storage._object_path(obj_id)

        assert object_path.startswith(self.path_prefix + "/")
        assert object_path in container


def _find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


try:
    from moto.server import ThreadedMotoServer
except ImportError:
    moto_available = False
else:
    moto_available = True


@pytest.mark.skipif(not moto_available, reason="moto package is not installed")
class TestMotoS3CloudObjStorage(ObjStorageTestFixture, unittest.TestCase):
    compression = "none"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.key = "testing"
        cls.secret = "testing"
        cls.host = "localhost"
        cls.port = _find_free_port()
        cls.server = ThreadedMotoServer(port=cls.port)
        cls.server.start()
        cls.container_name = secrets.token_hex(10)
        driver_cls = get_driver(Provider.S3)
        cls.driver = driver_cls(
            key=cls.key, secret=cls.secret, secure=False, host=cls.host, port=cls.port
        )
        cls.container = cls.driver.create_container(container_name=cls.container_name)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.driver.delete_container(cls.container)
        cls.server.stop()

    def setUp(self):
        super().setUp()

        self.storage = get_objstorage(
            "s3",
            container_name=self.container_name,
            compression=self.compression,
            key=self.key,
            secret=self.secret,
            secure=False,
            host=self.host,
            port=self.port,
        )

    def tearDown(self):
        super().tearDown()
        for obj in self.driver.list_container_objects(self.container):
            self.driver.delete_object(obj)
