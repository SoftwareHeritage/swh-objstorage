# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import tempfile
import unittest

from nose.plugins.attrib import attr

from swh.objstorage import get_objstorage
from swh.objstorage.tests.objstorage_testing import ObjStorageTestFixture
from swh.objstorage.tests.server_testing import ServerTestFixture
from swh.objstorage.api.server import app


@attr('db')
class TestRemoteObjStorage(ServerTestFixture, ObjStorageTestFixture,
                           unittest.TestCase):
    """ Test the remote archive API.
    """

    def setUp(self):
        self.config = {
            'cls': 'pathslicing',
            'args': {
                'root': tempfile.mkdtemp(),
                'slicing': '0:1/0:5',
            }
        }

        self.app = app
        super().setUp()
        self.storage = get_objstorage('remote', {
            'base_url': self.url()
        })