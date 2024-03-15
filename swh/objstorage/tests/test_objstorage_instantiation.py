# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.objstorage.api.client import RemoteObjStorage
from swh.objstorage.backends.pathslicing import PathSlicingObjStorage
from swh.objstorage.factory import get_objstorage


def test_pathslicing_objstorage(tmpdir):
    conf = {"cls": "pathslicing", "root": tmpdir, "slicing": "0:2/0:5"}
    st = get_objstorage(**conf)
    assert isinstance(st, PathSlicingObjStorage)


def test_remote_objstorage():
    conf = {"cls": "remote", "url": "http://127.0.0.1:4242/"}
    st = get_objstorage(**conf)
    assert isinstance(st, RemoteObjStorage)
