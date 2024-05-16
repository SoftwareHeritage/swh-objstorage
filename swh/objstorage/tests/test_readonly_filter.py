# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.objstorage.exc import (
    ObjCorruptedError,
    ObjNotFoundError,
    ReadOnlyObjStorageError,
)
from swh.objstorage.factory import get_objstorage
from swh.objstorage.objstorage import compute_hash


@pytest.fixture()
def objstorage(tmpdir):
    pstorage = {
        "cls": "pathslicing",
        "root": tmpdir,
        "slicing": "0:5",
    }
    base_storage = get_objstorage(**pstorage)
    storage = get_objstorage(
        "read-only",
        storage=base_storage,
    )
    return storage


@pytest.fixture()
def contentdata(objstorage):
    valid_content = b"pre-existing content"
    invalid_content = b"invalid_content"
    true_invalid_content = b"Anything that is not correct"
    absent_content = b"non-existent content"
    data = {
        "valid_content": valid_content,
        "valid_id": compute_hash(valid_content),
        "invalid_content": invalid_content,
        "true_invalid_content": true_invalid_content,
        "invalid_id": compute_hash(true_invalid_content),
        "absent_content": absent_content,
        "absent_id": compute_hash(absent_content),
    }
    # store one valid and one invalid content in the actual (backend) objstorage
    for cnt in ("valid", "invalid"):
        objstorage.storage.add(data[f"{cnt}_content"], obj_id=data[f"{cnt}_id"])
    return data


def test_can_contains(objstorage, contentdata):
    assert contentdata["valid_id"] in objstorage
    assert contentdata["invalid_id"] in objstorage
    assert contentdata["absent_id"] not in objstorage


def test_can_iter(objstorage, contentdata):
    assert {"sha1": contentdata["valid_id"]} in iter(objstorage)
    assert {"sha1": contentdata["invalid_id"]} in iter(objstorage)


def test_can_len(objstorage, contentdata):
    assert len(objstorage) == 2


def test_can_get(objstorage, contentdata):
    assert contentdata["valid_content"] == objstorage.get(contentdata["valid_id"])
    assert contentdata["invalid_content"] == objstorage.get(contentdata["invalid_id"])


def test_can_check(objstorage, contentdata):
    with pytest.raises(ObjNotFoundError):
        objstorage.check(contentdata["absent_id"])
    with pytest.raises(ObjCorruptedError):
        objstorage.check(contentdata["invalid_id"])
    objstorage.check(contentdata["valid_id"])


def test_cannot_add(objstorage, contentdata):
    with pytest.raises(ReadOnlyObjStorageError):
        objstorage.add(b"New content")
    with pytest.raises(ReadOnlyObjStorageError):
        objstorage.add(contentdata["valid_content"], contentdata["valid_id"])


def test_cannot_restore(objstorage, contentdata):
    with pytest.raises(ReadOnlyObjStorageError):
        objstorage.restore(contentdata["valid_content"], contentdata["valid_id"])


def test_check_config(objstorage):
    assert objstorage.check_config(check_write=False) is True
    assert objstorage.check_config(check_write=True) is False
