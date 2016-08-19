# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import tempfile
import unittest

from nose.tools import istest

from swh.objstorage.tests.server_testing import ServerTestFixture
from swh.objstorage import get_objstorage
from swh.objstorage.objstorage_pathslicing import PathSlicingObjStorage
from swh.objstorage.api.client import RemoteObjStorage
from swh.objstorage.api.server import app


class TestObjStorageInitialization(ServerTestFixture, unittest.TestCase):
    """ Test that the methods for ObjStorage initializations with
    `get_objstorage` works properly.
    """

    def setUp(self):
        self.path = tempfile.mkdtemp()
        # Server is launched at self.url()
        self.app = app
        self.config = {'storage_base': tempfile.mkdtemp(),
                       'storage_slicing': '0:1/0:5'}
        super().setUp()

    @istest
    def pathslicing_objstorage(self):
        conf = {
            'cls': 'pathslicing',
            'args': {'root': self.path, 'slicing': '0:2/0:5'}
        }
        st = get_objstorage(**conf)
        self.assertTrue(isinstance(st, PathSlicingObjStorage))

    @istest
    def remote_objstorage(self):
        conf = {
            'cls': 'remote',
            'args': {'base_url': self.url()}
        }
        st = get_objstorage(**conf)
        self.assertTrue(isinstance(st, RemoteObjStorage))
