# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import pytest

from swh.core.api.tests.server_testing import ServerTestFixture
from swh.objstorage.api.server import app
from swh.objstorage.factory import get_objstorage
from swh.objstorage.tests.objstorage_testing import ObjStorageTestFixture


class TestRemoteObjStorage(ServerTestFixture, ObjStorageTestFixture):
    """Test the remote archive API."""

    @pytest.fixture(autouse=True)
    def objstorage(self, tmpdir):
        self.config = {
            "objstorage": {
                "cls": "pathslicing",
                "root": str(tmpdir),
                "slicing": "0:1/0:5",
                "allow_delete": True,
            },
            "client_max_size": 8 * 1024 * 1024,
        }

        self.app = app
        self.start_server()
        self.storage = get_objstorage("remote", url=self.url())
        yield
        self.stop_server()

    @pytest.mark.skip("makes no sense to test this for the remote api")
    def test_delete_not_allowed(self):
        pass

    @pytest.mark.skip("makes no sense to test this for the remote api")
    def test_delete_not_allowed_by_default(self):
        pass
