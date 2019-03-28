# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import rados

from swh.model import hashutil

from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage import objstorage

READ_SIZE = 8192


class RADOSObjStorage(objstorage.ObjStorage):
    """Object storage implemented with RADOS"""

    def __init__(self, *, rados_id, pool_name, ceph_config,
                 allow_delete=False):
        super().__init__(allow_delete=allow_delete)
        self.pool_name = pool_name
        self.cluster = rados.Rados(
            conf=ceph_config,
            conffile='',
            rados_id=rados_id,
        )
        self.cluster.connect()
        self.__ioctx = None

    def check_config(self, *, check_write):
        if self.pool_name not in self.cluster.list_pools():
            raise ValueError('Pool %s does not exist' % self.pool_name)

    @staticmethod
    def _to_rados_obj_id(obj_id):
        """Convert to a RADOS object identifier"""
        return hashutil.hash_to_hex(obj_id)

    @property
    def ioctx(self):
        if not self.__ioctx:
            self.__ioctx = self.cluster.open_ioctx(self.pool_name)
        return self.__ioctx

    def __contains__(self, obj_id):
        try:
            self.ioctx.stat(self._to_rados_obj_id(obj_id))
        except rados.ObjectNotFound:
            return False
        else:
            return True

    def add(self, content, obj_id=None, check_presence=True):
        if not obj_id:
            raise ValueError('add needs an obj_id')

        _obj_id = self._to_rados_obj_id(obj_id)

        if check_presence:
            try:
                self.ioctx.stat(_obj_id)
            except rados.ObjectNotFound:
                pass
            else:
                return obj_id
        self.ioctx.write_full(_obj_id, content)

        return obj_id

    def get(self, obj_id):
        chunks = []
        _obj_id = self._to_rados_obj_id(obj_id)
        try:
            length, mtime = self.ioctx.stat(_obj_id)
        except rados.ObjectNotFound:
            raise ObjNotFoundError(obj_id) from None
        offset = 0
        while offset < length:
            chunk = self.ioctx.read(_obj_id, offset, READ_SIZE)
            chunks.append(chunk)
            offset += len(chunk)

        return b''.join(chunks)

    def check(self, obj_id):
        return True

    def delete(self, obj_id):
        super().delete(obj_id)  # check delete permission
        return True
