# Copyright (C) 2015-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os

import pytest

from swh.objstorage.exc import ObjCorruptedError
from swh.objstorage.objstorage import objid_to_default_hex

from .objstorage_testing import (
    FIRST_OBJID,
    ObjStorageTestFixture,
    assert_objid_lists_compatible,
)


class TestPathSlicingObjStorage(ObjStorageTestFixture):
    compression = "none"

    @pytest.fixture(autouse=True)
    def swh_objstorage_config(self, tmpdir):
        return {
            "cls": "pathslicing",
            "root": str(tmpdir),
            "slicing": "0:2/2:4/4:6",
            "compression": self.compression,
        }

    def content_path(self, obj_id):
        hex_obj_id = objid_to_default_hex(obj_id, self.storage.primary_hash)
        return self.storage.slicer.get_path(hex_obj_id)

    def test_iter(self):
        content, obj_id = self.hash_content(b"iter")
        assert not list(iter(self.storage))
        self.storage.add(content, obj_id=obj_id)
        assert list(iter(self.storage)) == [
            {self.storage.primary_hash: obj_id[self.storage.primary_hash]}
        ]

    def test_len(self):
        content, obj_id = self.hash_content(b"len")
        assert len(self.storage) == 0
        self.storage.add(content, obj_id=obj_id)
        assert len(self.storage) == 1

    def test_check_ok(self):
        content, obj_id = self.hash_content(b"check_ok")
        self.storage.add(content, obj_id=obj_id)
        assert self.storage.check(obj_id) is None

    def test_check_id_mismatch(self):
        _, obj_id = self.hash_content(b"check_id_mismatch")
        self.storage.add(b"unexpected content", obj_id=obj_id)
        with pytest.raises(ObjCorruptedError, match="Object corrupted"):
            self.storage.check(obj_id)

    def test_iterate_from(self):
        all_ids = []
        for i in range(100):
            content, obj_id = self.hash_content(b"content %d" % i)
            self.storage.add(content, obj_id=obj_id)
            all_ids.append(obj_id)
        all_ids.sort(key=lambda d: d[self.storage.primary_hash])

        ids = list(self.storage.iter_from(FIRST_OBJID))
        assert_objid_lists_compatible(ids, all_ids)

        ids = list(self.storage.iter_from(all_ids[0]))
        assert_objid_lists_compatible(ids, all_ids[1:])

        ids = list(self.storage.iter_from(all_ids[-1], n_leaf=True))
        n_leaf = ids[-1]
        ids = ids[:-1]
        assert n_leaf == 1
        assert len(ids) == 0

        ids = list(self.storage.iter_from(all_ids[-2], n_leaf=True))
        n_leaf = ids[-1]
        ids = ids[:-1]
        assert n_leaf == 2  # beware, this depends on the hash algo
        assert len(ids) == 1
        assert_objid_lists_compatible(ids, all_ids[-1:])

    def test_fdatasync_default(self, mocker):
        content, obj_id = self.hash_content(b"check_fdatasync")
        patched = mocker.patch.multiple(
            "os", fsync=mocker.DEFAULT, fdatasync=mocker.DEFAULT
        )
        self.storage.add(content, obj_id=obj_id)
        if self.storage.use_fdatasync:
            assert patched["fdatasync"].call_count == 1
            assert patched["fsync"].call_count == 0
        else:
            assert patched["fdatasync"].call_count == 0
            assert patched["fsync"].call_count == 1

    def test_fdatasync_forced_on(self, mocker):
        self.storage.use_fdatasync = True
        content, obj_id = self.hash_content(b"check_fdatasync")
        patched = mocker.patch.multiple(
            "os", fsync=mocker.DEFAULT, fdatasync=mocker.DEFAULT
        )
        self.storage.add(content, obj_id=obj_id)
        assert patched["fdatasync"].call_count == 1
        assert patched["fsync"].call_count == 0

    def test_fdatasync_forced_off(self, mocker):
        self.storage.use_fdatasync = False
        content, obj_id = self.hash_content(b"check_fdatasync")
        patched = mocker.patch.multiple(
            "os", fsync=mocker.DEFAULT, fdatasync=mocker.DEFAULT
        )
        self.storage.add(content, obj_id=obj_id)
        assert patched["fdatasync"].call_count == 0
        assert patched["fsync"].call_count == 1

    def test_check_not_compressed_trailing_data(self):
        content, obj_id = self.hash_content(b"check_not_compressed")
        self.storage.add(content, obj_id=obj_id)
        with open(self.content_path(obj_id), "ab") as f:  # Add garbage.
            f.write(b"garbage")
        with pytest.raises(ObjCorruptedError, match="Object corrupted") as error:
            self.storage.check(obj_id)
        if self.compression != "none":
            assert "trailing data found" in error.value.args[0]

    def test_check_not_compressed(self):
        content, obj_id = self.hash_content(b"check_not_compressed")
        self.storage.add(content, obj_id=obj_id)
        with open(self.content_path(obj_id), "wb") as f:  # Replace by garbage.
            f.write(b"garbage")
        with pytest.raises(ObjCorruptedError, match="Object corrupted") as error:
            self.storage.check(obj_id)
        if self.compression != "none":
            assert "not a proper compressed file" in error.value.args[0]

    def test__iter__skip_tmpfile(self, caplog):
        content, obj_id = self.hash_content(b"skip_tmpfile")
        self.storage.add(content, obj_id=obj_id)
        path = self.content_path(obj_id)

        dirname = os.path.dirname(path)
        bogus_path = os.path.join(dirname, "bogus_file")
        with open(bogus_path, "wb") as f:
            f.write(b"bogus")

        with caplog.at_level(logging.WARNING, "swh.objstorage.backends.pathslicing"):
            for _ in self.storage:
                pass

        assert len(caplog.records) == 1, [log.getMessage() for log in caplog.records]
        message = caplog.records[0].getMessage()
        assert "__iter__" in message
        assert bogus_path in message

    def test_iter_from_skip_tmpfile(self, caplog):
        content, obj_id = self.hash_content(b"skip_tmpfile")
        self.storage.add(content, obj_id=obj_id)
        path = self.content_path(obj_id)

        dirname = os.path.dirname(path)
        bogus_path = os.path.join(dirname, "bogus_file")
        with open(bogus_path, "wb") as f:
            f.write(b"bogus")

        with caplog.at_level(logging.WARNING, "swh.objstorage.backends.pathslicing"):
            for _ in self.storage.iter_from(FIRST_OBJID):
                pass

        assert len(caplog.records) == 1, [log.getMessage() for log in caplog.records]
        message = caplog.records[0].getMessage()
        assert "iter_from" in message
        assert bogus_path in message


class TestPathSlicingObjStorageGzip(TestPathSlicingObjStorage):
    compression = "gzip"


@pytest.mark.all_compression_methods
class TestPathSlicingObjStorageZlib(TestPathSlicingObjStorage):
    compression = "zlib"


@pytest.mark.all_compression_methods
class TestPathSlicingObjStorageBz2(TestPathSlicingObjStorage):
    compression = "bz2"


@pytest.mark.all_compression_methods
class TestPathSlicingObjStorageLzma(TestPathSlicingObjStorage):
    compression = "lzma"
