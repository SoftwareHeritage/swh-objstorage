# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil
import tempfile
import unittest

from swh.objstorage.backends.pathslicing import PathSlicingObjStorage
from swh.objstorage.multiplexer import MultiplexerObjStorage
from swh.objstorage.multiplexer.filter import add_filter, read_only

from .objstorage_testing import ObjStorageTestFixture


class TestMultiplexerObjStorage(ObjStorageTestFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.tmpdir = tempfile.mkdtemp()
        os.mkdir(os.path.join(self.tmpdir, "root1"))
        os.mkdir(os.path.join(self.tmpdir, "root2"))
        self.storage_v1 = PathSlicingObjStorage(
            os.path.join(self.tmpdir, "root1"), "0:2/2:4"
        )
        self.storage_v2 = PathSlicingObjStorage(
            os.path.join(self.tmpdir, "root2"), "0:1/0:5"
        )

        self.r_storage = add_filter(self.storage_v1, read_only())
        self.w_storage = self.storage_v2
        self.storage = MultiplexerObjStorage([self.r_storage, self.w_storage])

    def tearDown(self):
        super().tearDown()
        shutil.rmtree(self.tmpdir)

    def test_contains(self):
        content_p, obj_id_p = self.hash_content(b"contains_present")
        content_m, obj_id_m = self.hash_content(b"contains_missing")
        self.storage.add(content_p, obj_id=obj_id_p)
        self.assertIn(obj_id_p, self.storage)
        self.assertNotIn(obj_id_m, self.storage)

    def test_delete_missing(self):
        self.storage_v1.allow_delete = True
        self.storage_v2.allow_delete = True
        super().test_delete_missing()

    def test_delete_present(self):
        self.storage_v1.allow_delete = True
        self.storage_v2.allow_delete = True
        super().test_delete_present()

    def test_get_random_contents(self):
        content, obj_id = self.hash_content(b"get_random_content")
        self.storage.add(content)
        random_contents = list(self.storage.get_random(1))
        self.assertEqual(1, len(random_contents))
        self.assertIn(obj_id, random_contents)

    def test_access_readonly(self):
        # Add a content to the readonly storage
        content, obj_id = self.hash_content(b"content in read-only")
        self.storage_v1.add(content)
        # Try to retrieve it on the main storage
        self.assertIn(obj_id, self.storage)
