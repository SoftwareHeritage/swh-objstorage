# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import shutil
import tempfile
import unittest
from unittest.mock import DEFAULT, patch

from swh.model import hashutil
from swh.objstorage import exc
from swh.objstorage.factory import get_objstorage
from swh.objstorage.objstorage import ID_HASH_LENGTH

from .objstorage_testing import ObjStorageTestFixture


class TestPathSlicingObjStorage(ObjStorageTestFixture, unittest.TestCase):
    compression = "none"

    def setUp(self):
        super().setUp()
        self.slicing = "0:2/2:4/4:6"
        self.tmpdir = tempfile.mkdtemp()
        self.storage = get_objstorage(
            "pathslicing",
            {
                "root": self.tmpdir,
                "slicing": self.slicing,
                "compression": self.compression,
            },
        )

    def tearDown(self):
        super().tearDown()
        shutil.rmtree(self.tmpdir)

    def content_path(self, obj_id):
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        return self.storage._obj_path(hex_obj_id)

    def test_iter(self):
        content, obj_id = self.hash_content(b"iter")
        self.assertEqual(list(iter(self.storage)), [])
        self.storage.add(content, obj_id=obj_id)
        self.assertEqual(list(iter(self.storage)), [obj_id])

    def test_len(self):
        content, obj_id = self.hash_content(b"len")
        self.assertEqual(len(self.storage), 0)
        self.storage.add(content, obj_id=obj_id)
        self.assertEqual(len(self.storage), 1)

    def test_check_ok(self):
        content, obj_id = self.hash_content(b"check_ok")
        self.storage.add(content, obj_id=obj_id)
        assert self.storage.check(obj_id) is None
        assert self.storage.check(obj_id.hex()) is None

    def test_check_id_mismatch(self):
        content, obj_id = self.hash_content(b"check_id_mismatch")
        self.storage.add(b"unexpected content", obj_id=obj_id)
        with self.assertRaises(exc.Error) as error:
            self.storage.check(obj_id)
        self.assertEqual(
            (
                "Corrupt object %s should have id "
                "12ebb2d6c81395bcc5cab965bdff640110cb67ff" % obj_id.hex(),
            ),
            error.exception.args,
        )

    def test_get_random_contents(self):
        content, obj_id = self.hash_content(b"get_random_content")
        self.storage.add(content, obj_id=obj_id)
        random_contents = list(self.storage.get_random(1))
        self.assertEqual(1, len(random_contents))
        self.assertIn(obj_id, random_contents)

    def test_iterate_from(self):
        all_ids = []
        for i in range(100):
            content, obj_id = self.hash_content(b"content %d" % i)
            self.storage.add(content, obj_id=obj_id)
            all_ids.append(obj_id)
        all_ids.sort()

        ids = list(self.storage.iter_from(b"\x00" * (ID_HASH_LENGTH // 2)))
        self.assertEqual(len(ids), len(all_ids))
        self.assertEqual(ids, all_ids)

        ids = list(self.storage.iter_from(all_ids[0]))
        self.assertEqual(len(ids), len(all_ids) - 1)
        self.assertEqual(ids, all_ids[1:])

        ids = list(self.storage.iter_from(all_ids[-1], n_leaf=True))
        n_leaf = ids[-1]
        ids = ids[:-1]
        self.assertEqual(n_leaf, 1)
        self.assertEqual(len(ids), 0)

        ids = list(self.storage.iter_from(all_ids[-2], n_leaf=True))
        n_leaf = ids[-1]
        ids = ids[:-1]
        self.assertEqual(n_leaf, 2)  # beware, this depends on the hash algo
        self.assertEqual(len(ids), 1)
        self.assertEqual(ids, all_ids[-1:])

    def test_fdatasync_default(self):
        content, obj_id = self.hash_content(b"check_fdatasync")
        with patch.multiple("os", fsync=DEFAULT, fdatasync=DEFAULT) as patched:
            self.storage.add(content, obj_id=obj_id)
        if self.storage.use_fdatasync:
            assert patched["fdatasync"].call_count == 1
            assert patched["fsync"].call_count == 0
        else:
            assert patched["fdatasync"].call_count == 0
            assert patched["fsync"].call_count == 1

    def test_fdatasync_forced_on(self):
        self.storage.use_fdatasync = True
        content, obj_id = self.hash_content(b"check_fdatasync")
        with patch.multiple("os", fsync=DEFAULT, fdatasync=DEFAULT) as patched:
            self.storage.add(content, obj_id=obj_id)
        assert patched["fdatasync"].call_count == 1
        assert patched["fsync"].call_count == 0

    def test_fdatasync_forced_off(self):
        self.storage.use_fdatasync = False
        content, obj_id = self.hash_content(b"check_fdatasync")
        with patch.multiple("os", fsync=DEFAULT, fdatasync=DEFAULT) as patched:
            self.storage.add(content, obj_id=obj_id)
        assert patched["fdatasync"].call_count == 0
        assert patched["fsync"].call_count == 1

    def test_check_not_compressed(self):
        content, obj_id = self.hash_content(b"check_not_compressed")
        self.storage.add(content, obj_id=obj_id)
        with open(self.content_path(obj_id), "ab") as f:  # Add garbage.
            f.write(b"garbage")
        with self.assertRaises(exc.Error) as error:
            self.storage.check(obj_id)
        if self.compression == "none":
            self.assertIn("Corrupt object", error.exception.args[0])
        else:
            self.assertIn("trailing data found", error.exception.args[0])


class TestPathSlicingObjStorageGzip(TestPathSlicingObjStorage):
    compression = "gzip"


class TestPathSlicingObjStorageZlib(TestPathSlicingObjStorage):
    compression = "zlib"


class TestPathSlicingObjStorageBz2(TestPathSlicingObjStorage):
    compression = "bz2"


class TestPathSlicingObjStorageLzma(TestPathSlicingObjStorage):
    compression = "lzma"
