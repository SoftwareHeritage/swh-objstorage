# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import pytest

from swh.model import hashutil
from swh.objstorage.constants import ID_DIGEST_LENGTH
from swh.objstorage.exc import ObjCorruptedError

from .objstorage_testing import ObjStorageTestFixture


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
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        return self.storage.slicer.get_path(hex_obj_id)

    def test_iter(self):
        content, obj_id = self.hash_content(b"iter")
        assert not list(iter(self.storage))
        self.storage.add(content, obj_id=obj_id)
        assert list(iter(self.storage)) == [{self.storage.PRIMARY_HASH: obj_id}]

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
            all_ids.append({self.storage.PRIMARY_HASH: obj_id})
        all_ids.sort(key=lambda d: d[self.storage.PRIMARY_HASH])

        ids = list(self.storage.iter_from(b"\x00" * ID_DIGEST_LENGTH))
        assert ids == all_ids

        ids = list(self.storage.iter_from(all_ids[0]))
        assert ids == all_ids[1:]

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
        assert ids == all_ids[-1:]

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
