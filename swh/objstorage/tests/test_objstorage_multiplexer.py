# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil
import tempfile
import unittest

from nose.tools import istest

from swh.objstorage import PathSlicingObjStorage
from swh.objstorage.multiplexer import MultiplexerObjStorage
from swh.objstorage.multiplexer.filter import add_filter, read_only

from .objstorage_testing import ObjStorageTestFixture


class TestMultiplexerObjStorage(ObjStorageTestFixture, unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.tmpdir = tempfile.mkdtemp()
        os.mkdir(os.path.join(self.tmpdir, 'root1'))
        os.mkdir(os.path.join(self.tmpdir, 'root2'))
        self.storage_v1 = PathSlicingObjStorage(
            os.path.join(self.tmpdir, 'root1'), '0:2/2:4')
        self.storage_v2 = PathSlicingObjStorage(
            os.path.join(self.tmpdir, 'root2'), '0:1/0:5')

        self.r_storage = add_filter(self.storage_v1, read_only())
        self.w_storage = self.storage_v2
        self.storage = MultiplexerObjStorage([self.r_storage, self.w_storage])

    def tearDown(self):
        super().tearDown()
        shutil.rmtree(self.tmpdir)

    @istest
    def contains(self):
        content_p, obj_id_p = self.hash_content(b'contains_present')
        content_m, obj_id_m = self.hash_content(b'contains_missing')
        self.storage.add(content_p, obj_id=obj_id_p)
        self.assertIn(obj_id_p, self.storage)
        self.assertNotIn(obj_id_m, self.storage)

    @istest
    def iter(self):
        content, obj_id = self.hash_content(b'iter')
        self.assertEqual(list(iter(self.storage)), [])
        self.storage.add(content, obj_id=obj_id)
        self.assertEqual(list(iter(self.storage)), [obj_id])

    @istest
    def len(self):
        content, obj_id = self.hash_content(b'len')
        self.assertEqual(len(self.storage), 0)
        self.storage.add(content, obj_id=obj_id)
        self.assertEqual(len(self.storage), 1)

    @istest
    def len_multiple(self):
        content, obj_id = self.hash_content(b'len_multiple')
        # Add a content to the read-only storage
        self.storage_v1.add(content)
        self.assertEqual(len(self.storage), 1)
        # By adding the same content to the global storage, it should be
        # Replicated.
        # len() behavior is to indicates the number of files, not unique
        # contents.
        self.storage.add(content)
        self.assertEqual(len(self.storage), 2)

    @istest
    def delete_missing(self):
        self.storage_v1.allow_delete = True
        self.storage_v2.allow_delete = True
        super().delete_missing()

    @istest
    def delete_present(self):
        self.storage_v1.allow_delete = True
        self.storage_v2.allow_delete = True
        super().delete_present()

    @istest
    def get_random_contents(self):
        content, obj_id = self.hash_content(b'get_random_content')
        self.storage.add(content)
        random_contents = list(self.storage.get_random(1))
        self.assertEqual(1, len(random_contents))
        self.assertIn(obj_id, random_contents)

    @istest
    def access_readonly(self):
        # Add a content to the readonly storage
        content, obj_id = self.hash_content(b'content in read-only')
        self.storage_v1.add(content)
        # Try to retrieve it on the main storage
        self.assertIn(obj_id, self.storage)
