# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import re

import pytest

from swh.objstorage.backends.in_memory import InMemoryObjStorage
from swh.objstorage.exc import ObjCorruptedError, ReadOnlyObjStorageError
from swh.objstorage.multiplexer import MP_COUNTER_METRICS, MultiplexerObjStorage
from swh.objstorage.objstorage import DURATION_METRICS, compute_hashes

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
                    "name": "ro_backend",
                    "storage": {
                        "cls": "pathslicing",
                        "name": "pathslicer_1",
                        "root": root1,
                        "slicing": "0:2/2:4",
                    },
                },
                {
                    "cls": "pathslicing",
                    "name": "rw_backend",
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
        with pytest.raises(ReadOnlyObjStorageError):
            self.storage.storages[0].add(content, obj_id=obj_id)
        # Try to retrieve it on the main storage
        assert obj_id not in self.storage

    def test_get_statsd_multiplexer(self, statsd):
        content1, obj_id1 = self.compositehash_content(b"add_get_batch_1")
        content2, obj_id2 = self.compositehash_content(b"add_get_batch_2")
        content3, obj_id3 = self.compositehash_content(b"add_get_batch_3")
        self.storage.add(content1, obj_id1)
        self.storage.add(content2, obj_id2)
        expected_payloads = [
            # first call to add()
            f"{DURATION_METRICS}:#endpoint:add,name:multiplexer",
            # second call to add()
            f"{DURATION_METRICS}:#endpoint:add,name:multiplexer",
        ]
        payloads = [
            # only check statsd entried from the multiplexer itself
            re.sub(r"[:].*[|](ms|c)[|]", ":", s.decode(), 1)
            for s in statsd.socket.payloads
            if b"name:multiplexer" in s
        ]
        assert payloads == expected_payloads

        # add a content in the RO backend
        self.storage.storages[0].storage.add(content3, obj_id3)
        statsd.socket.payloads.clear()

        self.storage.get(obj_id1)
        self.storage.get(obj_id2)
        self.storage.get(obj_id3)

        # check metrics reported by the multiplexer itself
        expected_mp_payloads = [
            # first get()
            f"{MP_COUNTER_METRICS}:#backend:rw_backend,backend_number:1,endpoint:get,name:multiplexer",
            f"{DURATION_METRICS}:#endpoint:get,name:multiplexer",
            # second get()
            f"{MP_COUNTER_METRICS}:#backend:rw_backend,backend_number:1,endpoint:get,name:multiplexer",
            f"{DURATION_METRICS}:#endpoint:get,name:multiplexer",
            # third get()
            f"{MP_COUNTER_METRICS}:#backend:ro_backend,backend_number:0,endpoint:get,name:multiplexer",
            f"{DURATION_METRICS}:#endpoint:get,name:multiplexer",
        ]
        mp_payloads = [
            # only check statsd entried from the multiplexer itself
            re.sub(r"[:].*[|](ms|c)[|]", ":", s.decode(), 1)
            for s in statsd.socket.payloads
            if b"name:multiplexer" in s
        ]
        assert mp_payloads == expected_mp_payloads

        # check metrics reported by the ro backend (aka pathslicer_1)
        expected_ro_payloads = [
            # first get()
            f"{DURATION_METRICS}:#endpoint:__contains__,name:pathslicer_1",
            f"{DURATION_METRICS}_error_count:#endpoint:get,error_type:ObjNotFoundError,name:pathslicer_1",
            # second get()
            f"{DURATION_METRICS}:#endpoint:__contains__,name:pathslicer_1",
            f"{DURATION_METRICS}_error_count:#endpoint:get,error_type:ObjNotFoundError,name:pathslicer_1",
            # third get()
            f"{DURATION_METRICS}:#endpoint:__contains__,name:pathslicer_1",
            f"{DURATION_METRICS}:#endpoint:get,name:pathslicer_1",
        ]
        ro_payloads = [
            # only check statsd entried from the ro backend
            re.sub(r"[:].*[|](ms|c)[|]", ":", s.decode(), 1)
            for s in statsd.socket.payloads
            if b"name:pathslicer_1" in s
        ]
        assert ro_payloads == expected_ro_payloads

        # check metrics reported by the rw backend (aka rw_backend)
        expected_rw_payloads = [
            # first get()
            f"{DURATION_METRICS}:#endpoint:__contains__,name:rw_backend",
            f"{DURATION_METRICS}:#endpoint:get,name:rw_backend",
            # second get()
            f"{DURATION_METRICS}:#endpoint:__contains__,name:rw_backend",
            f"{DURATION_METRICS}:#endpoint:get,name:rw_backend",
        ]
        rw_payloads = [
            # only check statsd entried from the rw backend
            re.sub(r"[:].*[|](ms|c)[|]", ":", s.decode(), 1)
            for s in statsd.socket.payloads
            if b"name:rw_backend" in s
        ]
        assert rw_payloads == expected_rw_payloads

        # clear the statsd messages
        statsd.socket.payloads.clear()
        # and check metrics for the __contains__ method
        assert obj_id1 in self.storage
        assert obj_id2 in self.storage
        assert obj_id3 in self.storage

        # for now, there is no counter (MP_COUNTER_METRICS) on any other method
        # than get, so we don't have any simple stat on who answered the
        # request...
        expected_mp_payloads = [
            f"{DURATION_METRICS}:#endpoint:__contains__,name:multiplexer",
            f"{DURATION_METRICS}:#endpoint:__contains__,name:multiplexer",
            f"{DURATION_METRICS}:#endpoint:__contains__,name:multiplexer",
        ]
        mp_payloads = [
            # only check statsd entried from the multiplexer itself
            re.sub(r"[:].*[|](ms|c)[|]", ":", s.decode(), 1)
            for s in statsd.socket.payloads
            if b"name:multiplexer" in s
        ]
        assert mp_payloads == expected_mp_payloads

        expected_ro_payloads = [
            f"{DURATION_METRICS}:#endpoint:__contains__,name:pathslicer_1",
            f"{DURATION_METRICS}:#endpoint:__contains__,name:pathslicer_1",
            f"{DURATION_METRICS}:#endpoint:__contains__,name:pathslicer_1",
        ]
        ro_payloads = [
            # only check statsd entried from the ro backend
            re.sub(r"[:].*[|](ms|c)[|]", ":", s.decode(), 1)
            for s in statsd.socket.payloads
            if b"name:pathslicer_1" in s
        ]
        assert ro_payloads == expected_ro_payloads

        expected_rw_payloads = [
            f"{DURATION_METRICS}:#endpoint:__contains__,name:rw_backend",
            f"{DURATION_METRICS}:#endpoint:__contains__,name:rw_backend",
        ]
        rw_payloads = [
            # only check statsd entried from the ro backend
            re.sub(r"[:].*[|](ms|c)[|]", ":", s.decode(), 1)
            for s in statsd.socket.payloads
            if b"name:rw_backend" in s
        ]
        assert rw_payloads == expected_rw_payloads


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
