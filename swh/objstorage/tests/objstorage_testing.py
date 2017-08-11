# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import time

from nose.tools import istest

from swh.model import hashutil
from swh.objstorage import exc


class ObjStorageTestFixture():

    def setUp(self):
        super().setUp()

    def hash_content(self, content):
        obj_id = hashutil.hash_data(content)['sha1']
        return content, obj_id

    def assertContentMatch(self, obj_id, expected_content):  # noqa
        content = self.storage.get(obj_id)
        self.assertEqual(content, expected_content)

    @istest
    def check_config(self):
        self.assertTrue(self.storage.check_config(check_write=False))
        self.assertTrue(self.storage.check_config(check_write=True))

    @istest
    def contains(self):
        content_p, obj_id_p = self.hash_content(b'contains_present')
        content_m, obj_id_m = self.hash_content(b'contains_missing')
        self.storage.add(content_p, obj_id=obj_id_p)
        self.assertIn(obj_id_p, self.storage)
        self.assertNotIn(obj_id_m, self.storage)

    @istest
    def add_get_w_id(self):
        content, obj_id = self.hash_content(b'add_get_w_id')
        r = self.storage.add(content, obj_id=obj_id)
        self.assertEqual(obj_id, r)
        self.assertContentMatch(obj_id, content)

    @istest
    def add_big(self):
        content, obj_id = self.hash_content(b'add_big' * 1024 * 1024)
        r = self.storage.add(content, obj_id=obj_id)
        self.assertEqual(obj_id, r)
        self.assertContentMatch(obj_id, content)

    @istest
    def add_get_wo_id(self):
        content, obj_id = self.hash_content(b'add_get_wo_id')
        r = self.storage.add(content)
        self.assertEqual(obj_id, r)
        self.assertContentMatch(obj_id, content)

    @istest
    def add_get_batch(self):
        content1, obj_id1 = self.hash_content(b'add_get_batch_1')
        content2, obj_id2 = self.hash_content(b'add_get_batch_2')
        self.storage.add(content1, obj_id1)
        self.storage.add(content2, obj_id2)
        cr1, cr2 = self.storage.get_batch([obj_id1, obj_id2])
        self.assertEqual(cr1, content1)
        self.assertEqual(cr2, content2)

    @istest
    def get_batch_unexisting_content(self):
        content, obj_id = self.hash_content(b'get_batch_unexisting_content')
        result = list(self.storage.get_batch([obj_id]))
        self.assertTrue(len(result) == 1)
        self.assertIsNone(result[0])

    @istest
    def restore_content(self):
        valid_content, valid_obj_id = self.hash_content(b'restore_content')
        invalid_content = b'unexpected content'
        id_adding = self.storage.add(invalid_content, valid_obj_id)
        id_restore = self.storage.restore(valid_content)
        # Adding a false content then restore it to the right one and
        # then perform a verification should result in a successful check.
        self.assertEqual(id_adding, valid_obj_id)
        self.assertEqual(id_restore, valid_obj_id)
        self.assertContentMatch(valid_obj_id, valid_content)

    @istest
    def get_missing(self):
        content, obj_id = self.hash_content(b'get_missing')
        with self.assertRaises(exc.Error):
            self.storage.get(obj_id)

    @istest
    def check_missing(self):
        content, obj_id = self.hash_content(b'check_missing')
        with self.assertRaises(exc.Error):
            self.storage.check(obj_id)

    @istest
    def check_present(self):
        content, obj_id = self.hash_content(b'check_missing')
        self.storage.add(content)
        try:
            self.storage.check(obj_id)
        except:
            self.fail('Integrity check failed')

    @istest
    def delete_missing(self):
        content, obj_id = self.hash_content(b'missing_content_to_delete')
        with self.assertRaises(exc.Error):
            self.storage.delete(obj_id)

    @istest
    def delete_present(self):
        content, obj_id = self.hash_content(b'content_to_delete')
        self.storage.add(content, obj_id=obj_id)
        self.assertTrue(self.storage.delete(obj_id))
        with self.assertRaises(exc.Error):
            self.storage.get(obj_id)

    @istest
    def add_stream(self):
        content = [b'chunk1', b'chunk2']
        _, obj_id = self.hash_content(b''.join(content))
        try:
            self.storage.add_stream(iter(content), obj_id=obj_id)
        except NotImplementedError:
            return
        self.assertContentMatch(obj_id, b''.join(content))

    @istest
    def add_stream_sleep(self):
        def gen_content():
            yield b'chunk1'
            time.sleep(0.5)
            yield b'chunk2'
        _, obj_id = self.hash_content(b'placeholder_id')
        try:
            self.storage.add_stream(gen_content(), obj_id=obj_id)
        except NotImplementedError:
            return
        self.assertContentMatch(obj_id, b'chunk1chunk2')

    @istest
    def get_stream(self):
        content_l = [b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9']
        content = b''.join(content_l)
        _, obj_id = self.hash_content(content)
        self.storage.add(content, obj_id=obj_id)
        try:
            r = list(self.storage.get_stream(obj_id, chunk_size=1))
        except NotImplementedError:
            return
        self.assertEqual(r, content_l)
