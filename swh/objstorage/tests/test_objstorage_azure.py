# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import unittest
from unittest.mock import patch

from azure.common import AzureMissingResourceHttpError

from swh.objstorage import get_objstorage

from objstorage_testing import ObjStorageTestFixture


class MockBlob():
    """ Libcloud object mock that replicates its API """
    def __init__(self, name, content):
        self.name = name
        self.content = content


class MockBlockBlobService():
    """Mock internal azure library which AzureCloudObjStorage depends upon.

    """
    data = {}

    def __init__(self, account_name, account_key, **kwargs):
        # do not care for the account_name and the api_secret_key here
        self.data = defaultdict(dict)

    def get_container_properties(self, container_name):
        self.data[container_name]
        return container_name in self.data

    def create_blob_from_bytes(self, container_name, blob_name, blob):
        self.data[container_name][blob_name] = blob

    def get_blob_to_bytes(self, container_name, blob_name):
        if blob_name not in self.data[container_name]:
            raise AzureMissingResourceHttpError(
                'Blob %s not found' % blob_name,
                404)
        return MockBlob(name=blob_name,
                        content=self.data[container_name][blob_name])

    def delete_blob(self, container_name, blob_name):
        try:
            self.data[container_name].pop(blob_name)
        except KeyError:
            raise AzureMissingResourceHttpError(
                'Blob %s not found' % blob_name, 404)
        return True

    def exists(self, container_name, blob_name):
        return blob_name in self.data[container_name]

    def list_blobs(self, container_name):
        for blob_name, content in self.data[container_name].items():
            yield MockBlob(name=blob_name, content=content)


class TestAzureCloudObjStorage(ObjStorageTestFixture, unittest.TestCase):

    def setUp(self):
        super().setUp()
        patcher = patch(
            'swh.objstorage.cloud.objstorage_azure.BlockBlobService',
            MockBlockBlobService,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        self.storage = get_objstorage('azure-storage', {
            'account_name': 'account-name',
            'api_secret_key': 'api-secret-key',
            'container_name': 'container-name',
        })
