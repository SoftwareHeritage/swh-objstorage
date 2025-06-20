# Copyright (C) 2023-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from itertools import permutations

import pytest

from swh.objstorage.interface import COMPOSITE_OBJID_KEYS, ObjId, objid_from_dict


def test_composite_objid_keys():
    assert ObjId.__required_keys__ | ObjId.__optional_keys__ == COMPOSITE_OBJID_KEYS


def test_objid_from_dict_missing_key():
    with pytest.raises(ValueError, match="missing at least one of") as excinfo:
        _ = objid_from_dict({"foo": "bar"})

    for key in COMPOSITE_OBJID_KEYS:
        assert key in excinfo.value.args[0]


def test_objid_from_dict_valueerror():
    for key in COMPOSITE_OBJID_KEYS:
        with pytest.raises(TypeError, match=f"value for {key} is str, not bytes"):
            _ = objid_from_dict({key: "boom"})


def test_objid_from_dict():
    for i in range(len(COMPOSITE_OBJID_KEYS)):
        for j, keys in enumerate(permutations(COMPOSITE_OBJID_KEYS, i + 1)):
            d = {
                key: int.to_bytes(i * 1500 + j * 50 + k, 20, "little")
                for k, key in enumerate(keys)
            }

            objid = objid_from_dict(d)

            assert objid == d

            d2 = {**d, f"test{i:03x}": "garbage", f"extra{j:02d}": "extra"}

            assert objid_from_dict(d2) == d
