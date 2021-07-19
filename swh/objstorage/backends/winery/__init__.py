# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc

from swh.objstorage.exc import Error, ObjNotFoundError
from swh.objstorage.objstorage import ObjStorage, compute_hash


class WineryObjStorageDriver(object):
    pass


class WineryObjStorage(ObjStorage, metaclass=abc.ABCMeta):
    """ObjStorage that connect to a winery object storage

    https://wiki.softwareheritage.org/wiki/A_practical_approach_to_efficiently_store_100_billions_small_objects_in_Ceph

    Args:
      kwargs: extra arguments are passed through to the winery
    """

    def __init__(
        self, **kwargs,
    ):
        super().__init__(**kwargs)
        self.driver = WineryObjStorageDriver()

    def check_config(self, *, check_write):
        return True

    def __contains__(self, obj_id, *args, **kwargs):
        return obj_id in self.driver

    def add(self, content, obj_id=None, check_presence=True, *args, **kwargs):
        if check_presence is True:
            if obj_id in self.driver:
                return obj_id
        if obj_id is None:
            obj_id = compute_hash(content)
        self.driver[obj_id] = content
        return obj_id

    def get(self, obj_id, *args, **kwargs):
        if obj_id not in self.driver:
            raise ObjNotFoundError(obj_id)
        return self.driver[obj_id]

    def check(self, obj_id, *args, **kwargs):
        if obj_id not in self.driver:
            raise ObjNotFoundError(obj_id)
        if obj_id != compute_hash(self.driver[obj_id]):
            raise Error(obj_id)

    def delete(self, obj_id, *args, **kwargs):
        super().delete(obj_id)  # Check delete permission
        if obj_id not in self.driver:
            raise ObjNotFoundError(obj_id)
        del self.driver[obj_id]
        return True

    def __iter__(self):
        for k in sorted(self.driver.keys()):
            yield k
