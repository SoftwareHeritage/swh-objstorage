# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from swh.objstorage.factory import get_objstorage

from .objstorage_testing import ObjStorageTestFixture


class TestInMemoryObjStorage(ObjStorageTestFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.storage = get_objstorage(cls="memory", args={})
