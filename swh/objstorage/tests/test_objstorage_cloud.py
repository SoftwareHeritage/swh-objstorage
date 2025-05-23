# Copyright (C) 2016-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import closing
import socket
from typing import Optional

from libcloud.common.types import InvalidCredsError
from libcloud.storage.providers import get_driver
from libcloud.storage.types import (
    ContainerDoesNotExistError,
    ObjectDoesNotExistError,
    Provider,
)
import pytest

from swh.objstorage.backends.libcloud import CloudObjStorage
from swh.objstorage.exc import ObjCorruptedError
from swh.objstorage.factory import get_objstorage
from swh.objstorage.objstorage import decompressors

from .objstorage_testing import ObjStorageTestFixture

API_KEY = "API_KEY"
API_SECRET_KEY = "API SECRET KEY"
CONTAINER_NAME = "test_container"


@pytest.fixture(autouse=True)
def cloud_provider_mock(mocker):
    def _get_driver(self, **kwargs):
        return MockLibcloudDriver(**kwargs)

    mocker.patch(
        "swh.objstorage.backends.libcloud.CloudObjStorage._get_driver", _get_driver
    )
    mocker.patch(
        "swh.objstorage.backends.libcloud.CloudObjStorage._get_provider",
        lambda self: None,
    )


class MockLibcloudObject:
    """Libcloud object mock that replicates its API"""

    def __init__(self, name, content):
        self.name = name
        self.content = list(content)

    def as_stream(self):
        yield from iter(self.content)


class MockLibcloudDriver:
    """Mock driver that replicates the used LibCloud API"""

    def __init__(self, **kw):
        self.containers = {CONTAINER_NAME: {}}  # Storage is initialized
        self.api_key = kw.get("key", kw.get("api_key"))
        self.api_secret_key = kw.get("secret", kw.get("api_secret_key"))

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

    def get_object_cdn_url(self, obj, ex_expiry=24.0):
        # XXX tests should probably not pass with this implem...
        return None


class MockCloudObjStorage(CloudObjStorage):
    """Cloud object storage that uses a mocked driver"""

    def _get_provider(self):
        # Implement this for the abc requirement, but behavior is defined in
        # _get_driver.
        pass


class TestCloudObjStorage(ObjStorageTestFixture):
    compression = "none"
    path_prefix: Optional[str] = None

    @pytest.fixture(autouse=True)
    def objstorage(self):
        self.storage = get_objstorage(
            "s3",
            container_name=CONTAINER_NAME,
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

        with pytest.raises(ObjCorruptedError) as e:
            self.storage.check(obj_id)

        if self.compression != "none":
            assert "trailing data found" in e.value.args[0]

    @pytest.mark.skip("makes no sense to test this for the mocked libcloud")
    def test_download_url(self):
        pass


@pytest.mark.all_compression_methods
class TestCloudObjStorageBz2(TestCloudObjStorage):
    compression = "bz2"


class TestCloudObjStorageGzip(TestCloudObjStorage):
    compression = "gzip"


@pytest.mark.all_compression_methods
class TestCloudObjStorageLzma(TestCloudObjStorage):
    compression = "lzma"


@pytest.mark.all_compression_methods
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


@pytest.fixture(scope="class")
def moto_server():
    container_name = CONTAINER_NAME
    port = _find_free_port()
    moto_config = {
        "key": API_KEY,
        "secret": API_SECRET_KEY,
        "host": "localhost",
        "port": port,
        "secure": False,
    }
    server = ThreadedMotoServer(port=port)
    server.start()

    driver_cls = get_driver(Provider.S3)
    driver = driver_cls(**moto_config)
    container = driver.create_container(container_name=container_name)
    yield moto_config, driver, container
    driver.delete_container(container)
    server.stop()


@pytest.mark.skipif(not moto_available, reason="moto package is not installed")
class TestMotoS3CloudObjStorage(ObjStorageTestFixture):
    compression = "none"

    @pytest.fixture
    def swh_objstorage_config(self, moto_server):
        moto_config, driver, container = moto_server
        yield {
            "cls": "s3",
            "container_name": container.name,
            "compression": self.compression,
            **moto_config,
        }
        for obj in driver.list_container_objects(container):
            driver.delete_object(obj)
