# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random
import shutil
from string import ascii_lowercase
import tempfile
import unittest

from swh.objstorage.exc import Error, ObjNotFoundError
from swh.objstorage.factory import get_objstorage
from swh.objstorage.multiplexer.filter import read_only
from swh.objstorage.objstorage import compute_hash


def get_random_content():
    return bytes("".join(random.sample(ascii_lowercase, 10)), "utf8")


class ReadOnlyFilterTestCase(unittest.TestCase):
    # Read only filter should not allow writing

    def setUp(self):
        super().setUp()
        self.tmpdir = tempfile.mkdtemp()
        pstorage = {
            "cls": "pathslicing",
            "root": self.tmpdir,
            "slicing": "0:5",
        }
        base_storage = get_objstorage(**pstorage)
        self.storage = get_objstorage(
            "filtered", storage_conf=pstorage, filters_conf=[read_only()]
        )
        self.valid_content = b"pre-existing content"
        self.invalid_content = b"invalid_content"
        self.true_invalid_content = b"Anything that is not correct"
        self.absent_content = b"non-existent content"
        # Create a valid content.
        self.valid_id = compute_hash(self.valid_content)
        base_storage.add(self.valid_content, obj_id=self.valid_id)
        # Create an invalid id and add a content with it.
        self.invalid_id = compute_hash(self.true_invalid_content)
        base_storage.add(self.invalid_content, obj_id=self.invalid_id)
        # Compute an id for a non-existing content.
        self.absent_id = compute_hash(self.absent_content)

    def tearDown(self):
        super().tearDown()
        shutil.rmtree(self.tmpdir)

    def test_can_contains(self):
        self.assertTrue(self.valid_id in self.storage)
        self.assertTrue(self.invalid_id in self.storage)
        self.assertFalse(self.absent_id in self.storage)

    def test_can_iter(self):
        self.assertIn({"sha1": self.valid_id}, iter(self.storage))
        self.assertIn({"sha1": self.invalid_id}, iter(self.storage))

    def test_can_len(self):
        self.assertEqual(2, len(self.storage))

    def test_can_get(self):
        self.assertEqual(self.valid_content, self.storage.get(self.valid_id))
        self.assertEqual(self.invalid_content, self.storage.get(self.invalid_id))

    def test_can_check(self):
        with self.assertRaises(ObjNotFoundError):
            self.storage.check(self.absent_id)
        with self.assertRaises(Error):
            self.storage.check(self.invalid_id)
        self.storage.check(self.valid_id)

    def test_cannot_add(self):
        new_id = self.storage.add(b"New content")
        result = self.storage.add(self.valid_content, self.valid_id)
        self.assertIsNone(new_id, self.storage)
        self.assertIsNone(result)

    def test_cannot_restore(self):
        result = self.storage.restore(self.valid_content, self.valid_id)
        self.assertIsNone(result)
