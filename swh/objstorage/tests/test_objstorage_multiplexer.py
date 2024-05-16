# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os

import pytest

from swh.objstorage.backends.in_memory import InMemoryObjStorage
from swh.objstorage.exc import ObjCorruptedError
from swh.objstorage.multiplexer import MultiplexerObjStorage
from swh.objstorage.objstorage import compute_hashes

from .objstorage_testing import ObjStorageTestFixture


class TestMultiplexerObjStorage(ObjStorageTestFixture):
    @pytest.fixture
    def swh_objstorage_config(self, tmpdir):
        root1 = os.path.join(tmpdir, "root1")
        root2 = os.path.join(tmpdir, "root2")
        os.mkdir(root1)
        os.mkdir(root2)
        return {
            "cls": "multiplexer",
            "objstorages": [
                {
                    "cls": "read-only",
                    "storage": {
                        "cls": "pathslicing",
                        "root": root1,
                        "slicing": "0:2/2:4",
                    },
                },
                {
                    "cls": "pathslicing",
                    "root": root2,
                    "slicing": "0:1/0:5",
                },
            ],
        }

    def test_contains(self):
        content_p, obj_id_p = self.hash_content(b"contains_present")
        content_m, obj_id_m = self.hash_content(b"contains_missing")
        self.storage.add(content_p, obj_id=obj_id_p)
        assert obj_id_p in self.storage
        assert obj_id_m not in self.storage

    @pytest.fixture
    def allow_delete(self):
        for storage in self.storage.storages:
            storage.allow_delete = True

    def test_delete_missing(self, allow_delete):
        super().test_delete_missing()

    def test_delete_missing_composite(self, allow_delete):
        super().test_delete_missing_composite()

    def test_delete_present(self, allow_delete):
        super().test_delete_present()

    def test_delete_present_composite(self, allow_delete):
        super().test_delete_present_composite()

    def test_access_readonly(self):
        # Add a content to the readonly storage
        content, obj_id = self.hash_content(b"content in read-only")
        self.storage.storages[0].add(content, obj_id=obj_id)
        # Try to retrieve it on the main storage
        assert obj_id not in self.storage


def test_multiplexer_corruption_fallback(mocker, caplog):
    content_p = b"contains_present"
    obj_id_p = compute_hashes(content_p)

    class CorruptedInMemoryObjStorage(InMemoryObjStorage):
        name = "corrupted_objstorage"

        def get(self, obj_id):
            raise ObjCorruptedError("Always corrupted", obj_id)

    corrupt_storage = CorruptedInMemoryObjStorage()
    corrupt_get = mocker.spy(corrupt_storage, "get")

    ok_storage = InMemoryObjStorage()
    ok_get = mocker.spy(ok_storage, "get")

    multiplexer = MultiplexerObjStorage(objstorages=[corrupt_storage, ok_storage])
    multiplexer.add(content_p, obj_id=obj_id_p)

    assert obj_id_p in corrupt_storage
    assert obj_id_p in ok_storage

    with caplog.at_level(logging.WARNING, "swh.objstorage.multiplexer"):
        assert multiplexer.get(obj_id_p) == content_p

    corrupt_get.assert_called_once_with(obj_id_p)
    ok_get.assert_called_once_with(obj_id_p)

    assert len(caplog.records) == 1
    assert (
        "was reported as corrupted by backend 'corrupted_objstorage'"
        in caplog.records[0].message
    )
    for algo, hash in obj_id_p.items():
        assert f"{algo}:{hash.hex()}" in caplog.records[0].message
