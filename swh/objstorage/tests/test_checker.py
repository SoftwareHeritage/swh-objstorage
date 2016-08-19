# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import gzip
import tempfile
import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.core import hashutil
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.checker import RepairContentChecker


class MockBackupObjStorage():

    def __init__(self):
        self.values = {}

    def add(self, value, obj_id):
        self.values[obj_id] = value

    def get(self, obj_id):
        try:
            return self.values[obj_id]
        except KeyError:
            raise ObjNotFoundError(obj_id)


@attr('fs')
class TestRepairChecker(unittest.TestCase):
    """ Test the content integrity checker
    """

    def setUp(self):
        super().setUp()
        self._alter_config()
        self.checker = RepairContentChecker()
        self.checker.backups = [MockBackupObjStorage(),
                                MockBackupObjStorage()]

    def _alter_config(self):
        RepairContentChecker.parse_config_file = (
            lambda cls: {
                'storage': {'cls': 'pathslicing',
                            'args': {'root': tempfile.mkdtemp(),
                                     'slicing': '0:2/2:4/4:6'}},
                'batch_size': 1000,
                'log_tag': 'objstorage_test',
                'backup_storages': {}
            }
        )

    def _corrupt_content(self, obj_id):
        """ Make the given content invalid.
        """
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        file_path = self.checker.objstorage._obj_path(hex_obj_id)
        with gzip.open(file_path, 'wb') as f:
            f.write(b'Unexpected content')

    def _is_corrupted(self, obj_id):
        """ Ensure the given object is corrupted
        """
        return self.checker._check_content(obj_id) == 'corrupted'

    def _is_missing(self, obj_id):
        """ Ensure the given object is missing
        """
        return self.checker._check_content(obj_id) == 'missing'

    @istest
    def check_valid_content(self):
        # Check that a valid content is valid.
        content = b'check_valid_content'
        obj_id = self.checker.objstorage.add(content)
        self.assertFalse(self._is_corrupted(obj_id))
        self.assertFalse(self._is_missing(obj_id))

    @istest
    def check_corrupted_content(self):
        # Check that an invalid content is noticed.
        content = b'check_corrupted_content'
        obj_id = self.checker.objstorage.add(content)
        self._corrupt_content(obj_id)
        self.assertTrue(self._is_corrupted(obj_id))
        self.assertFalse(self._is_missing(obj_id))

    @istest
    def check_missing_content(self):
        obj_id = hashutil.hashdata(b'check_missing_content')['sha1']
        self.assertFalse(self._is_corrupted(obj_id))
        self.assertTrue(self._is_missing(obj_id))

    @istest
    def repair_content_present_first(self):
        # Try to repair a content that is in the backup storage.
        content = b'repair_content_present_first'
        obj_id = self.checker.objstorage.add(content)
        # Add a content to the mock
        self.checker.backups[0].add(content, obj_id)
        # Corrupt and repair it.
        self._corrupt_content(obj_id)
        self.assertTrue(self._is_corrupted(obj_id))
        self.checker.corrupted_content(obj_id)
        self.assertFalse(self._is_corrupted(obj_id))

    @istest
    def repair_content_present_second(self):
        # Try to repair a content that is in the backup storage.
        content = b'repair_content_present_first'
        obj_id = self.checker.objstorage.add(content)
        # Add a content to the mock
        self.checker.backups[-1].add(content, obj_id)
        # Corrupt and repair it.
        self._corrupt_content(obj_id)
        self.assertTrue(self._is_corrupted(obj_id))
        self.checker.corrupted_content(obj_id)
        self.assertFalse(self._is_corrupted(obj_id))

    @istest
    def repair_content_present_distributed(self):
        # Try to repair two contents that are in separate backup storages.
        content1 = b'repair_content_present_distributed_2'
        content2 = b'repair_content_present_distributed_1'
        obj_id1 = self.checker.objstorage.add(content1)
        obj_id2 = self.checker.objstorage.add(content2)
        # Add content to the mock.
        self.checker.backups[0].add(content1, obj_id1)
        self.checker.backups[1].add(content2, obj_id2)
        # Corrupt the contents
        self._corrupt_content(obj_id1)
        self._corrupt_content(obj_id2)
        self.assertTrue(self._is_corrupted(obj_id1))
        self.assertTrue(self._is_corrupted(obj_id2))
        # Repare them
        self.checker.corrupted_content(obj_id1)
        self.checker.corrupted_content(obj_id2)
        self.assertFalse(self._is_corrupted(obj_id1))
        self.assertFalse(self._is_corrupted(obj_id2))

    @istest
    def repair_content_missing(self):
        # Try to repair a content that is NOT in the backup storage.
        content = b'repair_content_missing'
        obj_id = self.checker.objstorage.add(content)
        # Corrupt the content
        self._corrupt_content(obj_id)
        self.assertTrue(self._is_corrupted(obj_id))
        # Try to repair it
        self.checker.corrupted_content(obj_id)
        self.assertTrue(self._is_corrupted(obj_id))
