# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from swh.objstorage.objstorage_in_memory import InMemoryObjStorage

from .objstorage_testing import ObjStorageTestFixture


class TestInMemoryObjStorage(ObjStorageTestFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.storage = InMemoryObjStorage()
