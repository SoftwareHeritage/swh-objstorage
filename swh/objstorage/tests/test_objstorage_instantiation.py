# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import shutil
import tempfile
import unittest

from swh.objstorage.api.client import RemoteObjStorage
from swh.objstorage.backends.pathslicing import PathSlicingObjStorage
from swh.objstorage.factory import get_objstorage


class TestObjStorageInitialization(unittest.TestCase):
    """ Test that the methods for ObjStorage initializations with
    `get_objstorage` works properly.
    """

    def setUp(self):
        self.path = tempfile.mkdtemp()
        self.path2 = tempfile.mkdtemp()
        # Server is launched at self.url()
        self.config = {"storage_base": self.path2, "storage_slicing": "0:1/0:5"}
        super().setUp()

    def tearDown(self):
        super().tearDown()
        shutil.rmtree(self.path)
        shutil.rmtree(self.path2)

    def test_pathslicing_objstorage(self):
        conf = {"cls": "pathslicing", "args": {"root": self.path, "slicing": "0:2/0:5"}}
        st = get_objstorage(**conf)
        self.assertTrue(isinstance(st, PathSlicingObjStorage))

    def test_remote_objstorage(self):
        conf = {"cls": "remote", "args": {"url": "http://127.0.0.1:4242/"}}
        st = get_objstorage(**conf)
        self.assertTrue(isinstance(st, RemoteObjStorage))
