# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
import unittest

from ..backends.winery import WineryObjStorage
from .objstorage_testing import ObjStorageTestFixture


class TestWineryObjStorage(ObjStorageTestFixture, unittest.TestCase):
    compression = "none"

    def setUp(self):
        super().setUp()
        self.url = "http://127.0.0.1/test"
        self.storage = WineryObjStorage()
        self.storage.driver = {}
