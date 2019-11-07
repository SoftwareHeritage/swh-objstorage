# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
from collections import defaultdict
from unittest.mock import patch

from typing import Any, Dict

from azure.common import AzureMissingResourceHttpError
from swh.model.hashutil import hash_to_hex

from swh.objstorage import get_objstorage
from swh.objstorage.objstorage import decompressors
from swh.objstorage.exc import Error

from .objstorage_testing import ObjStorageTestFixture


class MockBlob():
    """ Libcloud object mock that replicates its API """
    def __init__(self, name, content):
        self.name = name
        self.content = content


class MockBlockBlobService():
    """Mock internal azure library which AzureCloudObjStorage depends upon.

    """
    _data: Dict[str, Any] = {}

    def __init__(self, account_name, account_key, **kwargs):
        # do not care for the account_name and the api_secret_key here
        self._data = defaultdict(dict)

    def get_container_properties(self, container_name):
        self._data[container_name]
        return container_name in self._data

    def create_blob_from_bytes(self, container_name, blob_name, blob):
        self._data[container_name][blob_name] = blob

    def get_blob_to_bytes(self, container_name, blob_name):
        if blob_name not in self._data[container_name]:
            raise AzureMissingResourceHttpError(
                'Blob %s not found' % blob_name,
                404)
        return MockBlob(name=blob_name,
                        content=self._data[container_name][blob_name])

    def delete_blob(self, container_name, blob_name):
        try:
            self._data[container_name].pop(blob_name)
        except KeyError:
            raise AzureMissingResourceHttpError(
                'Blob %s not found' % blob_name, 404)
        return True

    def exists(self, container_name, blob_name):
        return blob_name in self._data[container_name]

    def list_blobs(self, container_name, marker=None, maxresults=None):
        for blob_name, content in sorted(self._data[container_name].items()):
            if marker is None or blob_name > marker:
                yield MockBlob(name=blob_name, content=content)


class TestAzureCloudObjStorage(ObjStorageTestFixture, unittest.TestCase):
    compression = 'none'

    def setUp(self):
        super().setUp()
        patcher = patch(
            'swh.objstorage.backends.azure.BlockBlobService',
            MockBlockBlobService,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        self.storage = get_objstorage('azure', {
            'account_name': 'account-name',
            'api_secret_key': 'api-secret-key',
            'container_name': 'container-name',
            'compression': self.compression,
        })

    def test_compression(self):
        content, obj_id = self.hash_content(b'test content is compressed')
        self.storage.add(content, obj_id=obj_id)

        blob_service, container = self.storage.get_blob_service(obj_id)
        internal_id = self.storage._internal_id(obj_id)

        raw_blob = blob_service.get_blob_to_bytes(container, internal_id)

        d = decompressors[self.compression]()
        assert d.decompress(raw_blob.content) == content
        assert d.unused_data == b''

    def test_trailing_data_on_stored_blob(self):
        content, obj_id = self.hash_content(b'test content without garbage')
        self.storage.add(content, obj_id=obj_id)

        blob_service, container = self.storage.get_blob_service(obj_id)
        internal_id = self.storage._internal_id(obj_id)

        blob_service._data[container][internal_id] += b'trailing garbage'

        if self.compression == 'none':
            with self.assertRaises(Error) as e:
                self.storage.check(obj_id)
        else:
            with self.assertRaises(Error) as e:
                self.storage.get(obj_id)
            assert 'trailing data' in e.exception.args[0]


class TestAzureCloudObjStorageGzip(TestAzureCloudObjStorage):
    compression = 'gzip'


class TestAzureCloudObjStorageZlib(TestAzureCloudObjStorage):
    compression = 'zlib'


class TestAzureCloudObjStorageLzma(TestAzureCloudObjStorage):
    compression = 'lzma'


class TestAzureCloudObjStorageBz2(TestAzureCloudObjStorage):
    compression = 'bz2'


class TestPrefixedAzureCloudObjStorage(ObjStorageTestFixture,
                                       unittest.TestCase):
    def setUp(self):
        super().setUp()
        patcher = patch(
            'swh.objstorage.backends.azure.BlockBlobService',
            MockBlockBlobService,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        self.accounts = {}
        for prefix in '0123456789abcdef':
            self.accounts[prefix] = {
                'account_name': 'account_%s' % prefix,
                'api_secret_key': 'secret_key_%s' % prefix,
                'container_name': 'container_%s' % prefix,
            }

        self.storage = get_objstorage('azure-prefixed', {
            'accounts': self.accounts
        })

    def test_prefixedazure_instantiation_missing_prefixes(self):
        del self.accounts['d']
        del self.accounts['e']

        with self.assertRaisesRegex(ValueError, 'Missing prefixes'):
            get_objstorage('azure-prefixed', {
                'accounts': self.accounts
            })

    def test_prefixedazure_instantiation_inconsistent_prefixes(self):
        self.accounts['00'] = self.accounts['0']

        with self.assertRaisesRegex(ValueError, 'Inconsistent prefixes'):
            get_objstorage('azure-prefixed', {
                'accounts': self.accounts
            })

    def test_prefixedazure_sharding_behavior(self):
        for i in range(100):
            content, obj_id = self.hash_content(b'test_content_%02d' % i)
            self.storage.add(content, obj_id=obj_id)
            hex_obj_id = hash_to_hex(obj_id)
            prefix = hex_obj_id[0]
            self.assertTrue(
                self.storage.prefixes[prefix][0].exists(
                    self.accounts[prefix]['container_name'], hex_obj_id
                ))
