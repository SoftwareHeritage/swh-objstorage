# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil
import tempfile
import unittest

from swh.objstorage.factory import get_objstorage

from .objstorage_testing import ObjStorageTestFixture


class TestStripingObjStorage(ObjStorageTestFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.base_dir = tempfile.mkdtemp()
        os.mkdir(os.path.join(self.base_dir, "root1"))
        os.mkdir(os.path.join(self.base_dir, "root2"))
        storage_config = {
            "cls": "striping",
            "args": {
                "objstorages": [
                    {
                        "cls": "pathslicing",
                        "args": {
                            "root": os.path.join(self.base_dir, "root1"),
                            "slicing": "0:2",
                            "allow_delete": True,
                        },
                    },
                    {
                        "cls": "pathslicing",
                        "args": {
                            "root": os.path.join(self.base_dir, "root2"),
                            "slicing": "0:2",
                            "allow_delete": True,
                        },
                    },
                ]
            },
        }
        self.storage = get_objstorage(**storage_config)

    def tearDown(self):
        shutil.rmtree(self.base_dir)

    def test_add_get_wo_id(self):
        self.skipTest("can't add without id in the multiplexer storage")

    def test_add_striping_behavior(self):
        exp_storage_counts = [0, 0]
        storage_counts = [0, 0]
        for i in range(100):
            content, obj_id = self.hash_content(b"striping_behavior_test%02d" % i)
            self.storage.add(content, obj_id)
            exp_storage_counts[self.storage.get_storage_index(obj_id)] += 1
            count = 0
            for i, storage in enumerate(self.storage.storages):
                if obj_id not in storage:
                    continue
                count += 1
                storage_counts[i] += 1
            self.assertEqual(count, 1)
        self.assertEqual(storage_counts, exp_storage_counts)

    def test_get_striping_behavior(self):
        # Make sure we can read objects that are available in any backend
        # storage
        content, obj_id = self.hash_content(b"striping_behavior_test")
        for storage in self.storage.storages:
            storage.add(content, obj_id)
            self.assertIn(obj_id, self.storage)
            storage.delete(obj_id)
            self.assertNotIn(obj_id, self.storage)

    def test_list_content(self):
        self.skipTest("Quite a chellenge to make it work")
