# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import shutil
import tempfile
import unittest

import pytest

from swh.core.api.tests.server_testing import ServerTestFixture
from swh.objstorage.api.server import app
from swh.objstorage.factory import get_objstorage
from swh.objstorage.tests.objstorage_testing import ObjStorageTestFixture


class TestRemoteObjStorage(ServerTestFixture, ObjStorageTestFixture, unittest.TestCase):
    """Test the remote archive API."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.config = {
            "objstorage": {
                "cls": "pathslicing",
                "root": self.tmpdir,
                "slicing": "0:1/0:5",
                "allow_delete": True,
            },
            "client_max_size": 8 * 1024 * 1024,
        }

        self.app = app
        super().setUp()
        self.storage = get_objstorage("remote", url=self.url())

    def tearDown(self):
        super().tearDown()
        shutil.rmtree(self.tmpdir)

    @pytest.mark.skip("makes no sense to test this for the remote api")
    def test_delete_not_allowed(self):
        pass

    @pytest.mark.skip("makes no sense to test this for the remote api")
    def test_delete_not_allowed_by_default(self):
        pass
