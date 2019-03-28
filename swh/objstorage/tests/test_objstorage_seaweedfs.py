# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from swh.objstorage.backends.seaweed import WeedObjStorage, DEFAULT_LIMIT
from swh.objstorage.tests.objstorage_testing import ObjStorageTestFixture


class MockWeedFiler:
    """ WeedFiler mock that replicates its API """
    def __init__(self, url):
        self.url = url
        self.content = {}

    def get(self, remote_path):
        return self.content[remote_path]

    def put(self, fp, remote_path):
        self.content[remote_path] = fp.read()

    def exists(self, remote_path):
        return remote_path in self.content

    def delete(self, remote_path):
        del self.content[remote_path]

    def list(self, dir, last_file_name=None, limit=DEFAULT_LIMIT):
        keys = sorted(self.content.keys())
        if last_file_name is None:
            idx = 0
        else:
            idx = keys.index(last_file_name) + 1
        return {'Entries': [{'FullPath': x} for x in keys[idx:idx+limit]]}


class TestWeedObjStorage(ObjStorageTestFixture, unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.url = 'http://127.0.0.1/test'
        self.storage = WeedObjStorage(url=self.url)
        self.storage.wf = MockWeedFiler(self.url)
