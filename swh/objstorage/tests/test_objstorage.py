# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import tempfile

from swh.objstorage import PathSlicingObjStorage

from swh.objstorage.tests.objstorage_testing import ObjStorageTestFixture


class TestObjStorage(ObjStorageTestFixture, unittest.TestCase):

    def setUp(self):
        self.storage = PathSlicingObjStorage(tempfile.mkdtemp(), '0:2/0:5')
