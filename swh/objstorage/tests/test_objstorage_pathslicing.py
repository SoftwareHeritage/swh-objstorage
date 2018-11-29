# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import shutil
import tempfile
import unittest

from swh.model import hashutil
from swh.objstorage import exc, get_objstorage

from .objstorage_testing import ObjStorageTestFixture


class TestPathSlicingObjStorage(ObjStorageTestFixture, unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.slicing = '0:2/2:4/4:6'
        self.tmpdir = tempfile.mkdtemp()
        self.storage = get_objstorage(
            'pathslicing',
            {'root': self.tmpdir, 'slicing': self.slicing}
        )

    def tearDown(self):
        super().tearDown()
        shutil.rmtree(self.tmpdir)

    def content_path(self, obj_id):
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        return self.storage._obj_path(hex_obj_id)

    def test_iter(self):
        content, obj_id = self.hash_content(b'iter')
        self.assertEqual(list(iter(self.storage)), [])
        self.storage.add(content, obj_id=obj_id)
        self.assertEqual(list(iter(self.storage)), [obj_id])

    def test_len(self):
        content, obj_id = self.hash_content(b'len')
        self.assertEqual(len(self.storage), 0)
        self.storage.add(content, obj_id=obj_id)
        self.assertEqual(len(self.storage), 1)

    def test_check_not_gzip(self):
        content, obj_id = self.hash_content(b'check_not_gzip')
        self.storage.add(content, obj_id=obj_id)
        with open(self.content_path(obj_id), 'ab') as f:  # Add garbage.
            f.write(b'garbage')
        with self.assertRaises(exc.Error):
            self.storage.check(obj_id)

    def test_check_id_mismatch(self):
        content, obj_id = self.hash_content(b'check_id_mismatch')
        self.storage.add(content, obj_id=obj_id)
        with open(self.content_path(obj_id), 'wb') as f:
            f.write(b'unexpected content')
        with self.assertRaises(exc.Error):
            self.storage.check(obj_id)

    def test_get_random_contents(self):
        content, obj_id = self.hash_content(b'get_random_content')
        self.storage.add(content, obj_id=obj_id)
        random_contents = list(self.storage.get_random(1))
        self.assertEqual(1, len(random_contents))
        self.assertIn(obj_id, random_contents)
