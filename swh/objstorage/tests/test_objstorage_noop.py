# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.objstorage.factory import get_objstorage
from swh.objstorage.objstorage import ObjStorage


def test_instantiate_noop_objstorage():
    objstorage = get_objstorage(cls="noop")
    assert objstorage is not None
    assert isinstance(objstorage, ObjStorage)
