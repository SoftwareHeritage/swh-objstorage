# Copyright (C) 2016-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio
import base64
import collections
from dataclasses import dataclass
import os
import secrets
import shutil
import subprocess
from urllib.parse import parse_qs, urlparse

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
import pytest

from swh.model.hashutil import hash_to_hex
import swh.objstorage.backends.azure
from swh.objstorage.exc import Error
from swh.objstorage.factory import get_objstorage
from swh.objstorage.objstorage import decompressors

from .objstorage_testing import ObjStorageTestFixture

AZURITE_EXE = shutil.which(
    "azurite-blob", path=os.environ.get("AZURITE_PATH", os.environ.get("PATH"))
)


@dataclass
class MockListedObject:
    name: str


class MockAsyncDownloadClient:
    def __init__(self, blob_data):
        self.blob_data = blob_data

    def content_as_bytes(self):
        future = asyncio.Future()
        future.set_result(self.blob_data)
        return future


class MockDownloadClient:
    def __init__(self, blob_data):
        self.blob_data = blob_data

    def content_as_bytes(self):
        return self.blob_data

    def __await__(self):
        yield from ()
        return MockAsyncDownloadClient(self.blob_data)


class MockBlobClient:
    def __init__(self, container, blob):
        self.container = container
        self.blob = blob

    def get_blob_properties(self):
        if self.blob not in self.container.blobs:
            raise ResourceNotFoundError("Blob not found")

        return {"exists": True}

    def upload_blob(self, data, length=None):
        if self.blob in self.container.blobs:
            raise ResourceExistsError("Blob already exists")

        if length is not None and length != len(data):
            raise ValueError("Wrong length for blob data!")

        self.container.blobs[self.blob] = data

    def download_blob(self):
        if self.blob not in self.container.blobs:
            raise ResourceNotFoundError("Blob not found")

        return MockDownloadClient(self.container.blobs[self.blob])

    def delete_blob(self):
        if self.blob not in self.container.blobs:
            raise ResourceNotFoundError("Blob not found")

        del self.container.blobs[self.blob]


@pytest.fixture(scope="class")
def azurite_connection_string(tmpdir_factory):
    host = "127.0.0.1"

    azurite_path = tmpdir_factory.mktemp("azurite")

    azurite_proc = subprocess.Popen(
        [
            AZURITE_EXE,
            "--blobHost",
            host,
            "--blobPort",
            "0",
        ],
        stdout=subprocess.PIPE,
        cwd=azurite_path,
    )

    prefix = b"Azurite Blob service successfully listens on "
    for line in azurite_proc.stdout:
        if line.startswith(prefix):
            base_url = line[len(prefix) :].decode().strip()
            break
    else:
        assert False, "Did not get Azurite Blob service port."

    # https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite#well-known-storage-account-and-key
    account_name = "devstoreaccount1"
    account_key = (
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq"
        "/K1SZFPTOtr/KBHBeksoGMGw=="
    )

    container_url = f"{base_url}/{account_name}"
    # note the stripping of the scheme for the BlobSecondaryEndpoint is NOT
    # a mistake; looks like azurite requires this secondary endpoint to not
    # come with the scheme (although I've not seen this documented, so not
    # sure if it's by design or a bug).
    secondary_url = container_url.replace("http://127.0.0.1", "localhost")

    yield (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={account_name};"
        f"AccountKey={account_key};"
        f"BlobEndpoint={container_url};"
        f"BlobSecondaryEndpoint={secondary_url};"
    )

    azurite_proc.kill()
    azurite_proc.wait(2)


@pytest.mark.skipif(not AZURITE_EXE, reason="azurite not found in AZURITE_PATH or PATH")
class TestAzuriteCloudObjStorage(ObjStorageTestFixture):
    compression = "none"

    @pytest.fixture(autouse=True)
    def objstorage(self, azurite_connection_string):
        self._container_name = secrets.token_hex(10)
        client = BlobServiceClient.from_connection_string(azurite_connection_string)
        client.create_container(self._container_name)

        self.storage = get_objstorage(
            "azure",
            connection_string=azurite_connection_string,
            container_name=self._container_name,
            compression=self.compression,
        )

    def test_download_url(self, azurite_connection_string):
        content_p, obj_id_p = self.hash_content(b"contains_present")
        self.storage.add(content_p, obj_id=obj_id_p)
        assert self.storage.download_url(obj_id_p).startswith("http://127.0.0.1:")
        storage2 = get_objstorage(
            "azure",
            connection_string=azurite_connection_string,
            container_name=self._container_name,
            compression=self.compression,
            use_secondary_endpoint_for_downloads=True,
        )
        assert storage2.download_url(obj_id_p).startswith("http://localhost:")


class TestAzuriteCloudObjStorageGzip(TestAzuriteCloudObjStorage):
    compression = "gzip"


def get_MockContainerClient():
    blobs = collections.defaultdict(dict)  # {container_url: {blob_id: blob}}

    class MockContainerClient:
        def __init__(self, container_url):
            self.container_url = container_url
            self.blobs = blobs[self.container_url]

        @classmethod
        def from_container_url(cls, container_url):
            return cls(container_url)

        def get_container_properties(self):
            return {"exists": True}

        def get_blob_client(self, blob):
            return MockBlobClient(self, blob)

        def list_blobs(self):
            for obj in sorted(self.blobs):
                yield MockListedObject(obj)

        def delete_blob(self, blob):
            self.get_blob_client(blob.name).delete_blob()

        def __aenter__(self):
            return self

        def __await__(self):
            future = asyncio.Future()
            future.set_result(self)
            yield from future

        def __aexit__(self, *args):
            return self

    return MockContainerClient


class TestMockedAzureCloudObjStorage(ObjStorageTestFixture):
    compression = "none"

    @pytest.fixture(autouse=True)
    def objstorage(self, mocker):
        ContainerClient = get_MockContainerClient()
        mocker.patch("swh.objstorage.backends.azure.ContainerClient", ContainerClient)

        mocker.patch(
            "swh.objstorage.backends.azure.AsyncContainerClient", ContainerClient
        )

        self.storage = get_objstorage(
            "azure",
            container_url="https://bogus-container-url.example",
            compression=self.compression,
        )

    def test_compression(self):
        content, obj_id = self.hash_content(b"test content is compressed")
        self.storage.add(content, obj_id=obj_id)

        internal_id = self.storage._internal_id(obj_id)
        blob_client = self.storage.get_blob_client(internal_id)
        raw_blob = blob_client.download_blob().content_as_bytes()

        d = decompressors[self.compression]()
        assert d.decompress(raw_blob) == content
        assert d.unused_data == b""

    def test_trailing_data_on_stored_blob(self):
        content, obj_id = self.hash_content(b"test content without garbage")
        self.storage.add(content, obj_id=obj_id)

        internal_id = self.storage._internal_id(obj_id)
        blob_client = self.storage.get_blob_client(internal_id)
        raw_blob = blob_client.download_blob().content_as_bytes()
        new_data = raw_blob + b"trailing garbage"

        blob_client.delete_blob()
        blob_client.upload_blob(data=new_data, length=len(new_data))

        if self.compression == "none":
            with pytest.raises(Error) as e:
                self.storage.check(obj_id)
        else:
            with pytest.raises(Error) as e:
                self.storage.get(obj_id)
            assert "trailing data" in e.value.args[0]

    @pytest.mark.skip("makes no sense to test this for the mocked azure")
    def test_download_url(self):
        pass


class TestMockedAzureCloudObjStorageGzip(TestMockedAzureCloudObjStorage):
    compression = "gzip"


@pytest.mark.all_compression_methods
class TestMockedAzureCloudObjStorageZlib(TestMockedAzureCloudObjStorage):
    compression = "zlib"


@pytest.mark.all_compression_methods
class TestMockedAzureCloudObjStorageLzma(TestMockedAzureCloudObjStorage):
    compression = "lzma"


@pytest.mark.all_compression_methods
class TestMockedAzureCloudObjStorageBz2(TestMockedAzureCloudObjStorage):
    compression = "bz2"


class TestPrefixedAzureCloudObjStorage(ObjStorageTestFixture):
    @pytest.fixture(autouse=True)
    def objstorage(self, mocker):
        self.ContainerClient = get_MockContainerClient()
        mocker.patch(
            "swh.objstorage.backends.azure.ContainerClient", self.ContainerClient
        )

        mocker.patch(
            "swh.objstorage.backends.azure.AsyncContainerClient", self.ContainerClient
        )

        self.accounts = {}
        for prefix in "0123456789abcdef":
            self.accounts[prefix] = "https://bogus-container-url.example/" + prefix

        self.storage = get_objstorage("azure-prefixed", accounts=self.accounts)

    def test_prefixedazure_instantiation_missing_prefixes(self):
        del self.accounts["d"]
        del self.accounts["e"]

        with pytest.raises(ValueError, match="Missing prefixes"):
            get_objstorage("azure-prefixed", accounts=self.accounts)

    def test_prefixedazure_instantiation_inconsistent_prefixes(self):
        self.accounts["00"] = self.accounts["0"]

        with pytest.raises(ValueError, match="Inconsistent prefixes"):
            get_objstorage("azure-prefixed", accounts=self.accounts)

    def test_prefixedazure_sharding_behavior(self):
        for i in range(100):
            content, obj_id = self.hash_content(b"test_content_%02d" % i)
            self.storage.add(content, obj_id=obj_id)
            hex_obj_id = hash_to_hex(obj_id)
            prefix = hex_obj_id[0]
            assert (
                self.ContainerClient(self.storage.container_urls[prefix])
                .get_blob_client(hex_obj_id)
                .get_blob_properties()
            )

    @pytest.mark.skip("makes no sense to test this for the mocked azure")
    def test_download_url(self):
        pass


def test_get_container_url():
    # r=read, l=list, w=write, d=delete
    policy_map = {
        "read_only": "rl",
        "append_only": "rwl",
        "full": "rwdl",
    }

    for policy, expected in policy_map.items():
        ret = swh.objstorage.backends.azure.get_container_url(
            account_name="account_name",
            account_key=base64.b64encode(b"account_key"),
            container_name="container_name",
            access_policy=policy,
        )

        p = urlparse(ret)
        assert p.scheme == "https"
        assert p.netloc == "account_name.blob.core.windows.net"
        assert p.path == "/container_name"

        qs = parse_qs(p.query)
        # sp: permissions
        assert qs["sp"] == [expected]
        # sr: resource (c=container)
        assert qs["sr"] == ["c"]
        # st: start; se: expiry
        assert qs["st"][0] < qs["se"][0]


def test_bwcompat_args(monkeypatch):
    monkeypatch.setattr(
        swh.objstorage.backends.azure,
        "ContainerClient",
        get_MockContainerClient(),
    )

    with pytest.deprecated_call():
        objs = get_objstorage(
            "azure",
            account_name="account_name",
            api_secret_key=base64.b64encode(b"account_key"),
            container_name="container_name",
        )

    assert objs is not None


def test_bwcompat_args_prefixed(monkeypatch):
    monkeypatch.setattr(
        swh.objstorage.backends.azure,
        "ContainerClient",
        get_MockContainerClient(),
    )

    accounts = {
        prefix: {
            "account_name": f"account_name{prefix}",
            "api_secret_key": base64.b64encode(b"account_key"),
            "container_name": "container_name",
        }
        for prefix in "0123456789abcdef"
    }

    with pytest.deprecated_call():
        objs = get_objstorage("azure-prefixed", accounts=accounts)

    assert objs is not None
