# Copyright (C) 2015-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import inspect
from typing import Iterable, Optional

import pytest
import requests

from swh.core.config import get_swh_backend_module, list_swh_backends
from swh.objstorage.exc import ObjCorruptedError, ObjNotFoundError
from swh.objstorage.interface import ObjId, ObjStorageInterface
from swh.objstorage.objstorage import decompressors, objid_for_content

FIRST_OBJID = {
    "sha1": b"\x00" * 20,
    "sha1_git": b"\x00" * 20,
    "sha256": b"\x00" * 32,
    "blake2s256": "\x00" * 32,
}
LAST_OBJID = {
    "sha1": b"\xff" * 20,
    "sha1_git": b"\xff" * 20,
    "sha256": b"\xff" * 32,
    "blake2s256": "\xff" * 32,
}


def get_cls(sto: ObjStorageInterface) -> Optional[str]:
    """Helper function to find the 'cls' name associated with a given
    ObjStorage object (mostly a reverse of the OBJSTORAGE_IMPLEMENTATIONS dict
    defined in factory.py)
    """
    for backend in list_swh_backends("objstorage"):
        mod, cls = get_swh_backend_module("objstorage", backend)
        if sto.__class__ is cls:
            return backend
    return None


def assert_objid_lists_compatible(
    iter1: Iterable[ObjId], iter2: Iterable[ObjId]
) -> None:
    """Check that two object id lists are compatible: same length,
    and hashes are subsets of one another"""
    list1 = list(iter1)
    list2 = list(iter2)
    assert len(list1) == len(list2), f"Mismatched lengths {len(list1)} != {len(list2)}"

    for left, right in zip(list1, list2):
        keys = set(left).intersection(set(right))
        assert keys, f"{left} and {right} have no keys in common"
        for key in keys:
            assert left[key] == right[key], (  # type: ignore[literal-required]
                f"{left} and {right} have mismatched {key}.\n"
                + f"{key} for list1:\n"
                + "\n".join(i[key].hex() for i in list1)  # type: ignore[literal-required]
                + f"\n{key} for list2\n"
                + "\n".join(i[key].hex() for i in list2)  # type: ignore[literal-required]
            )


class ObjStorageTestFixture:
    num_objects = 1200

    @pytest.fixture(autouse=True)
    def objstorage(self, swh_objstorage):
        self.storage = swh_objstorage

    def fill_objstorage(self, num_objects):
        all_ids = []
        for i in range(num_objects):
            content = b"content %d" % i
            obj_id = objid_for_content(content)
            self.storage.add(content, obj_id, check_presence=False)
            all_ids.append(obj_id)
        all_ids.sort(key=lambda d: d[self.storage.PRIMARY_HASH])
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

    def test_name(self):
        assert self.storage.name == get_cls(self.storage)

    def hash_content(self, content):
        obj_id = objid_for_content(content)
        return content, obj_id

    def assertContentMatch(self, obj_id, expected_content):  # noqa
        content = self.storage.get(obj_id)
        assert content == expected_content

    def test_check_config(self):
        assert self.storage.check_config(check_write=False)
        assert self.storage.check_config(check_write=True)

    def test_contains(self):
        content_p, obj_id_p = self.hash_content(b"contains_present")
        content_m, obj_id_m = self.hash_content(b"contains_missing")
        self.storage.add(content_p, obj_id=obj_id_p)
        assert obj_id_p in self.storage
        assert obj_id_m not in self.storage

    def test_add_get_w_id(self):
        content, obj_id = self.hash_content(b"add_get_w_id")
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

    def test_add_statsd(self, statsd):
        content, obj_id = self.hash_content(b"add_get_w_id")
        self.storage.add(content, obj_id=obj_id)
        self.check_statsd(statsd, "add")

    def test_add_get_batch(self):
        content1, obj_id1 = self.hash_content(b"add_get_batch_1")
        content2, obj_id2 = self.hash_content(b"add_get_batch_2")
        self.storage.add(content1, obj_id1)
        self.storage.add(content2, obj_id2)
        cr1, cr2 = self.storage.get_batch([obj_id1, obj_id2])
        assert cr1 == content1
        assert cr2 == content2

    def test_get_batch_unexisting_content(self):
        content, obj_id = self.hash_content(b"get_batch_unexisting_content")
        result = list(self.storage.get_batch([obj_id]))
        assert len(result) == 1
        assert result[0] is None

    def check_statsd(self, statsd, endpoint):
        while True:
            stats = statsd.socket.recv()
            assert stats
            value, unit, tags = stats.split("|")
            if (
                f"name:{self.storage.name}" in tags
                and f"endpoint:{endpoint}" in tags
                and value.startswith("swh_objstorage_request_duration_seconds:")
            ):
                assert unit == "ms"
                return
        assert False, "Missing expected statsd message"

    def test_get_statsd(self, statsd):
        content1, obj_id1 = self.hash_content(b"add_get_batch_1")
        content2, obj_id2 = self.hash_content(b"add_get_batch_2")
        self.storage.add(content1, obj_id1)
        self.storage.add(content2, obj_id2)
        while statsd.socket.recv():
            pass
        content = self.storage.get(obj_id1)
        assert content == content1
        content = self.storage.get(obj_id2)
        assert content == content2
        content = self.storage.get(obj_id2)
        content3, obj_id3 = self.hash_content(b"get_missing")
        with pytest.raises(ObjNotFoundError):
            self.storage.get(obj_id3)

        for endpoint in ("get", "get", "get"):
            self.check_statsd(statsd, endpoint)

    def test_restore_content(self):
        self.storage.allow_delete = True

        valid_content, valid_obj_id = self.hash_content(b"restore_content")
        invalid_content = b"unexpected content"
        self.storage.add(invalid_content, valid_obj_id)
        with pytest.raises((ObjCorruptedError, ObjNotFoundError)):
            # raise Corrupted except read only storage that raises NotFound,
            self.storage.check(valid_obj_id)
        assert valid_obj_id in self.storage
        self.storage.restore(valid_content, valid_obj_id)
        self.assertContentMatch(valid_obj_id, valid_content)

    def test_get_missing(self):
        content, obj_id = self.hash_content(b"get_missing")
        with pytest.raises(ObjNotFoundError) as e:
            self.storage.get(obj_id)

        assert obj_id in e.value.args

    def test_check_missing(self):
        content, obj_id = self.hash_content(b"check_missing")
        with pytest.raises(ObjNotFoundError):
            self.storage.check(obj_id)

    def test_check_present(self):
        content, obj_id = self.hash_content(b"check_present")
        self.storage.add(content, obj_id)
        try:
            self.storage.check(obj_id)
        except ObjCorruptedError:
            self.fail("Integrity check failed")

    def test_delete_missing(self):
        self.storage.allow_delete = True
        content, obj_id = self.hash_content(b"missing_content_to_delete")
        with pytest.raises(ObjNotFoundError):
            self.storage.delete(obj_id)

    def test_delete_present(self):
        self.storage.allow_delete = True
        content, obj_id = self.hash_content(b"content_to_delete")
        self.storage.add(content, obj_id=obj_id)
        assert self.storage.delete(obj_id)
        with pytest.raises(ObjNotFoundError):
            self.storage.get(obj_id)

    def test_delete_not_allowed(self):
        self.storage.allow_delete = False
        content, obj_id = self.hash_content(b"content_to_delete")
        self.storage.add(content, obj_id=obj_id)
        with pytest.raises(PermissionError):
            self.storage.delete(obj_id)

    def test_delete_not_allowed_by_default(self):
        content, obj_id = self.hash_content(b"content_to_delete")
        self.storage.add(content, obj_id=obj_id)
        with pytest.raises(PermissionError):
            self.storage.delete(obj_id)

    def test_add_batch(self):
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

        assert ret == {
            "object:add": expected_content_add,
            "object:add:bytes": expected_content_add_bytes,
        }

        for obj_id, content in contents:
            assert obj_id in self.storage

    def test_content_iterator(self):
        sto_obj_ids = list(iter(self.storage))
        assert not sto_obj_ids
        obj_ids = self.fill_objstorage(self.num_objects)

        sto_obj_ids = list(self.storage)
        assert_objid_lists_compatible(obj_ids, sto_obj_ids)

    def test_download_url(self):
        content = b"foo"
        obj_id = objid_for_content(content)
        self.storage.add(content, obj_id)
        url = self.storage.download_url(obj_id)
        if url is not None:
            decompress = decompressors[self.storage.compression]().decompress
            assert decompress(requests.get(url).content) == content

            with pytest.raises(ObjNotFoundError):
                self.storage.download_url(LAST_OBJID)
