# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from swh.objstorage.backends.seaweed import DEFAULT_LIMIT, WeedObjStorage
from swh.objstorage.exc import Error
from swh.objstorage.objstorage import decompressors
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
        return {"Entries": [{"FullPath": x} for x in keys[idx : idx + limit]]}


class TestWeedObjStorage(ObjStorageTestFixture, unittest.TestCase):
    compression = "none"

    def setUp(self):
        super().setUp()
        self.url = "http://127.0.0.1/test"
        self.storage = WeedObjStorage(url=self.url, compression=self.compression)
        self.storage.wf = MockWeedFiler(self.url)

    def test_compression(self):
        content, obj_id = self.hash_content(b"test compression")
        self.storage.add(content, obj_id=obj_id)

        raw_content = self.storage.wf.get(self.storage._path(obj_id))

        d = decompressors[self.compression]()
        assert d.decompress(raw_content) == content
        assert d.unused_data == b""

    def test_trailing_data_on_stored_blob(self):
        content, obj_id = self.hash_content(b"test content without garbage")
        self.storage.add(content, obj_id=obj_id)

        path = self.storage._path(obj_id)
        self.storage.wf.content[path] += b"trailing garbage"

        if self.compression == "none":
            with self.assertRaises(Error) as e:
                self.storage.check(obj_id)
        else:
            with self.assertRaises(Error) as e:
                self.storage.get(obj_id)
            assert "trailing data" in e.exception.args[0]


class TestWeedObjStorageBz2(TestWeedObjStorage):
    compression = "bz2"


class TestWeedObjStorageGzip(TestWeedObjStorage):
    compression = "gzip"


class TestWeedObjStorageLzma(TestWeedObjStorage):
    compression = "lzma"


class TestWeedObjStorageZlib(TestWeedObjStorage):
    compression = "zlib"
