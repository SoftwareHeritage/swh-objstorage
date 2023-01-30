# Copyright (C) 2016-2022  The Software Heritage developers
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
import tempfile
import unittest
from unittest.mock import patch
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


@pytest.mark.skipif(not AZURITE_EXE, reason="azurite not found in AZURITE_PATH or PATH")
class TestAzuriteCloudObjStorage(ObjStorageTestFixture, unittest.TestCase):
    compression = "none"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        host = "127.0.0.1"

        cls._azurite_path = tempfile.mkdtemp()

        cls._azurite_proc = subprocess.Popen(
            [
                AZURITE_EXE,
                "--blobHost",
                host,
                "--blobPort",
                "0",
            ],
            stdout=subprocess.PIPE,
            cwd=cls._azurite_path,
        )

        prefix = b"Azurite Blob service successfully listens on "
        for line in cls._azurite_proc.stdout:
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
        cls._connection_string = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={account_name};"
            f"AccountKey={account_key};"
            f"BlobEndpoint={container_url};"
        )

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls._azurite_proc.kill()
        cls._azurite_proc.wait(2)
        shutil.rmtree(cls._azurite_path)

    def setUp(self):
        super().setUp()
        self._container_name = secrets.token_hex(10)
        client = BlobServiceClient.from_connection_string(self._connection_string)
        client.create_container(self._container_name)

        self.storage = get_objstorage(
            "azure",
            connection_string=self._connection_string,
            container_name=self._container_name,
            compression=self.compression,
        )


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


class TestMockedAzureCloudObjStorage(ObjStorageTestFixture, unittest.TestCase):
    compression = "none"

    def setUp(self):
        super().setUp()
        ContainerClient = get_MockContainerClient()
        patcher = patch(
            "swh.objstorage.backends.azure.ContainerClient", ContainerClient
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        patcher = patch(
            "swh.objstorage.backends.azure.AsyncContainerClient", ContainerClient
        )
        patcher.start()
        self.addCleanup(patcher.stop)

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
            with self.assertRaises(Error) as e:
                self.storage.check(obj_id)
        else:
            with self.assertRaises(Error) as e:
                self.storage.get(obj_id)
            assert "trailing data" in e.exception.args[0]


class TestMockedAzureCloudObjStorageGzip(TestMockedAzureCloudObjStorage):
    compression = "gzip"


class TestMockedAzureCloudObjStorageZlib(TestMockedAzureCloudObjStorage):
    compression = "zlib"


class TestMockedAzureCloudObjStorageLzma(TestMockedAzureCloudObjStorage):
    compression = "lzma"


class TestMockedAzureCloudObjStorageBz2(TestMockedAzureCloudObjStorage):
    compression = "bz2"


class TestPrefixedAzureCloudObjStorage(ObjStorageTestFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.ContainerClient = get_MockContainerClient()
        patcher = patch(
            "swh.objstorage.backends.azure.ContainerClient", self.ContainerClient
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        patcher = patch(
            "swh.objstorage.backends.azure.AsyncContainerClient", self.ContainerClient
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        self.accounts = {}
        for prefix in "0123456789abcdef":
            self.accounts[prefix] = "https://bogus-container-url.example/" + prefix

        self.storage = get_objstorage("azure-prefixed", accounts=self.accounts)

    def test_prefixedazure_instantiation_missing_prefixes(self):
        del self.accounts["d"]
        del self.accounts["e"]

        with self.assertRaisesRegex(ValueError, "Missing prefixes"):
            get_objstorage("azure-prefixed", accounts=self.accounts)

    def test_prefixedazure_instantiation_inconsistent_prefixes(self):
        self.accounts["00"] = self.accounts["0"]

        with self.assertRaisesRegex(ValueError, "Inconsistent prefixes"):
            get_objstorage("azure-prefixed", accounts=self.accounts)

    def test_prefixedazure_sharding_behavior(self):
        for i in range(100):
            content, obj_id = self.hash_content(b"test_content_%02d" % i)
            self.storage.add(content, obj_id=obj_id)
            hex_obj_id = hash_to_hex(obj_id)
            prefix = hex_obj_id[0]
            self.assertTrue(
                self.ContainerClient(self.storage.container_urls[prefix])
                .get_blob_client(hex_obj_id)
                .get_blob_properties()
            )


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
