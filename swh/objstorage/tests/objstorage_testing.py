# Copyright (C) 2015-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import inspect
from typing import Tuple

from swh.objstorage import exc
from swh.objstorage.interface import CompositeObjId, ObjStorageInterface
from swh.objstorage.objstorage import compute_hash


class ObjStorageTestFixture:
    num_objects = 1200

    def fill_objstorage(self, num_objects):
        all_ids = []
        for i in range(num_objects):
            content = b"content %d" % i
            obj_id = compute_hash(content)
            self.storage.add(content, obj_id, check_presence=False)
            all_ids.append({"sha1": obj_id})
        all_ids.sort(key=lambda d: d["sha1"])
        return all_ids

    def test_types(self):
        """Checks all methods of ObjStorageInterface are implemented by this
        backend, and that they have the same signature."""
        # Create an instance of the protocol (which cannot be instantiated
        # directly, so this creates a subclass, then instantiates it)
        interface = type("_", (ObjStorageInterface,), {})()

        assert "get_batch" in dir(interface)

        missing_methods = []

        for meth_name in dir(interface):
            if meth_name.startswith("_") and meth_name not in (
                "__iter__",
                "__contains__",
            ):
                continue
            interface_meth = getattr(interface, meth_name)
            concrete_meth = getattr(self.storage, meth_name)

            expected_signature = inspect.signature(interface_meth)
            actual_signature = inspect.signature(concrete_meth)

            assert expected_signature == actual_signature, meth_name

        assert missing_methods == []

        # If all the assertions above succeed, then this one should too.
        # But there's no harm in double-checking.
        # And we could replace the assertions above by this one, but unlike
        # the assertions above, it doesn't explain what is missing.
        assert isinstance(self.storage, ObjStorageInterface)

    def hash_content(self, content):
        obj_id = compute_hash(content)
        return content, obj_id

    def compositehash_content(self, content) -> Tuple[bytes, CompositeObjId]:
        obj_id = compute_hash(content)
        return content, {"sha1": obj_id}

    def assertContentMatch(self, obj_id, expected_content):  # noqa
        content = self.storage.get(obj_id)
        self.assertEqual(content, expected_content)

    def test_check_config(self):
        self.assertTrue(self.storage.check_config(check_write=False))
        self.assertTrue(self.storage.check_config(check_write=True))

    def test_contains(self):
        content_p, obj_id_p = self.hash_content(b"contains_present")
        content_m, obj_id_m = self.hash_content(b"contains_missing")
        self.storage.add(content_p, obj_id=obj_id_p)
        self.assertIn(obj_id_p, self.storage)
        self.assertNotIn(obj_id_m, self.storage)

    def test_contains_composite(self):
        content_p, obj_id_p = self.compositehash_content(b"contains_present")
        content_m, obj_id_m = self.compositehash_content(b"contains_missing")
        self.storage.add(content_p, obj_id=obj_id_p)
        self.assertIn(obj_id_p, self.storage)
        self.assertNotIn(obj_id_m, self.storage)

    def test_add_get_w_id(self):
        content, obj_id = self.hash_content(b"add_get_w_id")
        self.storage.add(content, obj_id=obj_id)
        self.assertContentMatch(obj_id, content)

    def test_add_get_w_composite_id(self):
        content, obj_id = self.compositehash_content(b"add_get_w_id")
        self.storage.add(content, obj_id=obj_id)
        self.assertContentMatch(obj_id, content)

    def test_add_twice(self):
        content, obj_id = self.hash_content(b"add_twice")
        self.storage.add(content, obj_id=obj_id)
        self.assertContentMatch(obj_id, content)
        self.storage.add(content, obj_id=obj_id, check_presence=False)
        self.assertContentMatch(obj_id, content)

    def test_add_big(self):
        content, obj_id = self.hash_content(b"add_big" * 1024 * 1024)
        self.storage.add(content, obj_id=obj_id)
        self.assertContentMatch(obj_id, content)

    def test_add_get_batch(self):
        content1, obj_id1 = self.hash_content(b"add_get_batch_1")
        content2, obj_id2 = self.hash_content(b"add_get_batch_2")
        self.storage.add(content1, obj_id1)
        self.storage.add(content2, obj_id2)
        cr1, cr2 = self.storage.get_batch([obj_id1, obj_id2])
        self.assertEqual(cr1, content1)
        self.assertEqual(cr2, content2)

    def test_add_get_batch_composite(self):
        content1, obj_id1 = self.compositehash_content(b"add_get_batch_1")
        content2, obj_id2 = self.compositehash_content(b"add_get_batch_2")
        self.storage.add(content1, obj_id1)
        self.storage.add(content2, obj_id2)
        cr1, cr2 = self.storage.get_batch([obj_id1, obj_id2])
        self.assertEqual(cr1, content1)
        self.assertEqual(cr2, content2)

    def test_get_batch_unexisting_content(self):
        content, obj_id = self.hash_content(b"get_batch_unexisting_content")
        result = list(self.storage.get_batch([obj_id]))
        self.assertTrue(len(result) == 1)
        self.assertIsNone(result[0])

    def test_restore_content(self):
        self.storage.allow_delete = True

        valid_content, valid_obj_id = self.hash_content(b"restore_content")
        invalid_content = b"unexpected content"
        self.storage.add(invalid_content, valid_obj_id)
        with self.assertRaises(exc.Error):
            self.storage.check(valid_obj_id)
        self.storage.restore(valid_content, valid_obj_id)
        self.assertContentMatch(valid_obj_id, valid_content)

    def test_get_missing(self):
        content, obj_id = self.hash_content(b"get_missing")
        with self.assertRaises(exc.ObjNotFoundError) as e:
            self.storage.get(obj_id)

        self.assertIn(obj_id, e.exception.args)

    def test_get_missing_composite(self):
        content, obj_id = self.compositehash_content(b"get_missing")
        with self.assertRaises(exc.ObjNotFoundError) as e:
            self.storage.get(obj_id)

        self.assertIn(obj_id, e.exception.args)

    def test_check_missing(self):
        content, obj_id = self.hash_content(b"check_missing")
        with self.assertRaises(exc.Error):
            self.storage.check(obj_id)

    def test_check_missing_composite(self):
        content, obj_id = self.compositehash_content(b"check_missing")
        with self.assertRaises(exc.Error):
            self.storage.check(obj_id)

    def test_check_present(self):
        content, obj_id = self.hash_content(b"check_present")
        self.storage.add(content, obj_id)
        try:
            self.storage.check(obj_id)
        except exc.Error:
            self.fail("Integrity check failed")

    def test_check_present_composite(self):
        content, obj_id = self.compositehash_content(b"check_present")
        self.storage.add(content, obj_id)
        try:
            self.storage.check(obj_id)
        except exc.Error:
            self.fail("Integrity check failed")

    def test_delete_missing(self):
        self.storage.allow_delete = True
        content, obj_id = self.hash_content(b"missing_content_to_delete")
        with self.assertRaises(exc.Error):
            self.storage.delete(obj_id)

    def test_delete_missing_composite(self):
        self.storage.allow_delete = True
        content, obj_id = self.compositehash_content(b"missing_content_to_delete")
        with self.assertRaises(exc.Error):
            self.storage.delete(obj_id)

    def test_delete_present(self):
        self.storage.allow_delete = True
        content, obj_id = self.hash_content(b"content_to_delete")
        self.storage.add(content, obj_id=obj_id)
        self.assertTrue(self.storage.delete(obj_id))
        with self.assertRaises(exc.Error):
            self.storage.get(obj_id)

    def test_delete_present_composite(self):
        self.storage.allow_delete = True
        content, obj_id = self.compositehash_content(b"content_to_delete")
        self.storage.add(content, obj_id=obj_id)
        self.assertTrue(self.storage.delete(obj_id))
        with self.assertRaises(exc.Error):
            self.storage.get(obj_id)

    def test_delete_not_allowed(self):
        self.storage.allow_delete = False
        content, obj_id = self.hash_content(b"content_to_delete")
        self.storage.add(content, obj_id=obj_id)
        with self.assertRaises(PermissionError):
            self.storage.delete(obj_id)

    def test_delete_not_allowed_by_default(self):
        content, obj_id = self.hash_content(b"content_to_delete")
        self.storage.add(content, obj_id=obj_id)
        with self.assertRaises(PermissionError):
            self.assertTrue(self.storage.delete(obj_id))

    def test_add_batch(self):
        contents = {}
        expected_content_add = 0
        expected_content_add_bytes = 0
        for i in range(50):
            content = b"Test content %02d" % i
            content, obj_id = self.hash_content(content)
            contents[obj_id] = content
            expected_content_add_bytes += len(content)
            expected_content_add += 1

        ret = self.storage.add_batch(contents)

        self.assertEqual(
            ret,
            {
                "object:add": expected_content_add,
                "object:add:bytes": expected_content_add_bytes,
            },
        )
        for obj_id in contents:
            self.assertIn(obj_id, self.storage)

    def test_add_batch_list(self):
        contents = []
        expected_content_add = 0
        expected_content_add_bytes = 0
        for i in range(50):
            content = b"Test content %02d" % i
            content, obj_id = self.hash_content(content)
            contents.append((obj_id, content))
            expected_content_add_bytes += len(content)
            expected_content_add += 1

        ret = self.storage.add_batch(contents)

        self.assertEqual(
            ret,
            {
                "object:add": expected_content_add,
                "object:add:bytes": expected_content_add_bytes,
            },
        )
        for obj_id, content in contents:
            self.assertIn(obj_id, self.storage)

    def test_add_batch_list_composite(self):
        contents = []
        expected_content_add = 0
        expected_content_add_bytes = 0
        for i in range(50):
            content = b"Test content %02d" % i
            content, obj_id = self.compositehash_content(content)
            contents.append((obj_id, content))
            expected_content_add_bytes += len(content)
            expected_content_add += 1

        ret = self.storage.add_batch(contents)

        self.assertEqual(
            ret,
            {
                "object:add": expected_content_add,
                "object:add:bytes": expected_content_add_bytes,
            },
        )
        for obj_id, content in contents:
            self.assertIn(obj_id, self.storage)

    def test_content_iterator(self):
        sto_obj_ids = list(iter(self.storage))
        self.assertFalse(sto_obj_ids)
        obj_ids = self.fill_objstorage(self.num_objects)

        sto_obj_ids = list(self.storage)
        assert len(sto_obj_ids) == len(obj_ids)
        assert sto_obj_ids == obj_ids

    def test_list_content_all(self):
        assert self.num_objects > 100
        all_ids = self.fill_objstorage(self.num_objects)

        ids = list(self.storage.list_content(limit=None))
        assert len(ids) == len(all_ids)
        assert ids == all_ids

    def test_list_content_limit(self):
        assert self.num_objects > 100
        all_ids = self.fill_objstorage(self.num_objects)

        ids = list(self.storage.list_content(limit=10))
        assert len(ids) == 10
        assert ids == all_ids[:10]

    def test_list_content_limit_and_last(self):
        assert self.num_objects > 110
        all_ids = self.fill_objstorage(self.num_objects)

        id0 = self.num_objects - 105
        ids = list(self.storage.list_content(last_obj_id=all_ids[id0], limit=100))
        assert len(ids) == 100
        assert ids == all_ids[id0 + 1 : id0 + 101]

        # check proper behavior at the end of the range
        id0 = self.num_objects - 51
        ids = list(self.storage.list_content(last_obj_id=all_ids[id0], limit=100))
        assert len(ids) == 50
        assert ids == all_ids[-50:]

        # check proper behavior after the end of the range
        ids = list(self.storage.list_content(last_obj_id=all_ids[-1], limit=100))
        assert not ids

        ids = list(
            self.storage.list_content(last_obj_id={"sha1": b"\xff" * 20}, limit=100)
        )
        assert not ids
