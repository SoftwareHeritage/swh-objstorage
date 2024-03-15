# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random
from string import ascii_lowercase

import pytest

from swh.objstorage.exc import Error, ObjNotFoundError
from swh.objstorage.factory import get_objstorage
from swh.objstorage.multiplexer.filter import read_only
from swh.objstorage.objstorage import compute_hash


def get_random_content():
    return bytes("".join(random.sample(ascii_lowercase, 10)), "utf8")


class ReadOnlyFilterTestCase:
    # Read only filter should not allow writing

    @pytest.fixture(autouse=True)
    def objstorage(self, tmpdir):
        pstorage = {
            "cls": "pathslicing",
            "root": tmpdir,
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

    def test_can_contains(self):
        assert self.valid_id in self.storage
        assert self.invalid_id in self.storage
        assert self.absent_id not in self.storage

    def test_can_iter(self):
        assert {"sha1": self.valid_id} in iter(self.storage)
        assert {"sha1": self.invalid_id} in iter(self.storage)

    def test_can_len(self):
        assert len(self.storage) == 2

    def test_can_get(self):
        assert self.valid_content == self.storage.get(self.valid_id)
        assert self.invalid_content == self.storage.get(self.invalid_id)

    def test_can_check(self):
        with pytest.raises(ObjNotFoundError):
            self.storage.check(self.absent_id)
        with pytest.raises(Error):
            self.storage.check(self.invalid_id)
        self.storage.check(self.valid_id)

    def test_cannot_add(self):
        new_id = self.storage.add(b"New content")
        result = self.storage.add(self.valid_content, self.valid_id)
        assert new_id is None
        assert result is None

    def test_cannot_restore(self):
        result = self.storage.restore(self.valid_content, self.valid_id)
        assert result is None
