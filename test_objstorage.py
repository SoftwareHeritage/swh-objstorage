# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import gzip
import os
import shutil
import stat
import tempfile
import unittest

from io import BytesIO
from nose.tools import istest

from swh.core import hashutil
from swh.storage import objstorage


class TestObjStorage(unittest.TestCase):

    def setUp(self):
        self.content = b'42\n'

        # sha1
        self.hex_obj_id = '34973274ccef6ab4dfaaf86599792fa9c3fe4689'

        # sha1_git
        # self.hex_obj_id = 'd81cc0710eb6cf9efd5b920a8453e1e07157b6cd'

        self.obj_id = hashutil.hex_to_hash(self.hex_obj_id)
        self.obj_steps = [self.hex_obj_id[0:2], self.hex_obj_id[2:4],
                          self.hex_obj_id[4:6]]
        self.obj_relpath = os.path.join(*(self.obj_steps + [self.hex_obj_id]))

        self.tmpdir = tempfile.mkdtemp()
        self.obj_dirs = [
            os.path.join(self.tmpdir, *self.obj_steps[:i])
            for i in range(1, len(self.obj_steps))
        ]
        self.obj_path = os.path.join(self.tmpdir, self.obj_relpath)

        self.storage = objstorage.ObjStorage(root=self.tmpdir, depth=3)

        self.missing_obj_id = hashutil.hex_to_hash(
            'f1d2d2f924e986ac86fdf7b36c94bcdf32beec15')

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def assertGzipContains(self, gzip_path, content):  # noqa
        self.assertEqual(gzip.open(gzip_path, 'rb').read(), content)

    @istest
    def add_bytes_w_id(self):
        r = self.storage.add_bytes(self.content, obj_id=self.obj_id)
        self.assertEqual(r, self.obj_id)
        self.assertGzipContains(self.obj_path, self.content)

    @istest
    def add_bytes_wo_id(self):
        r = self.storage.add_bytes(self.content)
        self.assertEqual(r, self.obj_id)
        self.assertGzipContains(self.obj_path, self.content)

    @istest
    def add_file_w_id(self):
        r = self.storage.add_file(BytesIO(self.content),
                                  len(self.content),
                                  obj_id=self.obj_id)
        self.assertEqual(r, self.obj_id)
        self.assertGzipContains(self.obj_path, self.content)

    @istest
    def add_file_wo_id(self):
        r = self.storage.add_file(BytesIO(self.content), len(self.content))
        self.assertEqual(r, self.obj_id)
        self.assertGzipContains(self.obj_path, self.content)

    @istest
    def contains(self):
        self.storage.add_bytes(self.content, obj_id=self.obj_id)
        self.assertIn(self.obj_id, self.storage)
        self.assertNotIn(self.missing_obj_id, self.storage)

    @istest
    def check_ok(self):
        self.storage.add_bytes(self.content, obj_id=self.obj_id)
        try:
            self.storage.check(self.obj_id)
        except:
            self.fail('integrity check failed')

    @istest
    def check_missing(self):
        with self.assertRaises(objstorage.Error):
            self.storage.check(self.obj_id)

    @istest
    def check_file_and_dirs_mode(self):
        old_umask = os.umask(0)
        self.storage.add_bytes(self.content, obj_id=self.obj_id)
        for dir in self.obj_dirs:
            stat_dir = os.stat(dir)
            self.assertEquals(stat.S_IMODE(stat_dir.st_mode),
                              objstorage.DIR_MODE)
        stat_res = os.stat(self.obj_path)
        self.assertEquals(stat.S_IMODE(stat_res.st_mode), objstorage.FILE_MODE)
        os.umask(old_umask)

    @istest
    def check_not_gzip(self):
        self.storage.add_bytes(self.content, obj_id=self.obj_id)
        with open(self.obj_path, 'ab') as f:  # add trailing garbage
            f.write(b'garbage')
        with self.assertRaises(objstorage.Error):
            self.storage.check(self.obj_id)

    @istest
    def check_id_mismatch(self):
        self.storage.add_bytes(self.content, obj_id=self.obj_id)
        with gzip.open(self.obj_path, 'wb') as f:  # replace gzipped content
            f.write(b'unexpected content')
        with self.assertRaises(objstorage.Error):
            self.storage.check(self.obj_id)

    @istest
    def get_bytes(self):
        self.storage.add_bytes(self.content, obj_id=self.obj_id)
        self.assertEqual(self.storage.get_bytes(self.obj_id),
                         self.content)

    @istest
    def get_file_path(self):
        self.storage.add_bytes(self.content, obj_id=self.obj_id)
        path = self.storage._get_file_path(self.obj_id)
        self.assertEqual(os.path.basename(path), self.hex_obj_id)
        self.assertEqual(gzip.open(path, 'rb').read(), self.content)

    @istest
    def get_missing(self):
        with self.assertRaises(objstorage.Error):
            with self.storage.get_file_obj(self.missing_obj_id) as f:
                f.read()

    @istest
    def iter(self):
        self.assertEqual(list(iter(self.storage)), [])
        self.storage.add_bytes(self.content, obj_id=self.obj_id)
        self.assertEqual(list(iter(self.storage)), [self.obj_id])

    @istest
    def len(self):
        self.assertEqual(len(self.storage), 0)
        self.storage.add_bytes(self.content, obj_id=self.obj_id)
        self.assertEqual(len(self.storage), 1)
