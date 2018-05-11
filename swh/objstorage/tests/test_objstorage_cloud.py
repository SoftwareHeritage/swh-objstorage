# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from swh.objstorage.cloud.objstorage_cloud import CloudObjStorage
from libcloud.storage.types import (ObjectDoesNotExistError,
                                    ContainerDoesNotExistError)
from libcloud.common.types import InvalidCredsError

from .objstorage_testing import ObjStorageTestFixture


API_KEY = 'API_KEY'
API_SECRET_KEY = 'API SECRET KEY'
CONTAINER_NAME = 'test_container'


class MockLibcloudObject():
    """ Libcloud object mock that replicates its API """
    def __init__(self, name, content):
        self.name = name
        self.content = list(content)

    def as_stream(self):
        yield from iter(self.content)


class MockLibcloudDriver():
    """ Mock driver that replicates the used LibCloud API """
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
            raise ContainerDoesNotExistError(container_name=container_name,
                                             driver=self, value=None)

    def iterate_container_objects(self, container):
        self._check_credentials()
        yield from container.values()

    def get_object(self, container_name, obj_id):
        self._check_credentials()
        try:
            container = self.get_container(container_name)
            return container[obj_id]
        except KeyError:
            raise ObjectDoesNotExistError(object_name=obj_id,
                                          driver=self, value=None)

    def delete_object(self, obj):
        self._check_credentials()
        try:
            container = self.get_container(CONTAINER_NAME)
            container.pop(obj.name)
            return True
        except KeyError:
            raise ObjectDoesNotExistError(object_name=obj.name,
                                          driver=self, value=None)

    def upload_object_via_stream(self, content, container, obj_id):
        self._check_credentials()
        obj = MockLibcloudObject(obj_id, content)
        container[obj_id] = obj


class MockCloudObjStorage(CloudObjStorage):
    """ Cloud object storage that uses a mocked driver """
    def _get_driver(self, api_key, api_secret_key):
        return MockLibcloudDriver(api_key, api_secret_key)

    def _get_provider(self):
        # Implement this for the abc requirement, but behavior is defined in
        # _get_driver.
        pass


class TestCloudObjStorage(ObjStorageTestFixture, unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.storage = MockCloudObjStorage(API_KEY, API_SECRET_KEY,
                                           CONTAINER_NAME)
