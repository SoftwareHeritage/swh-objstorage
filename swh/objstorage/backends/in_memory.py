# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Iterator

from swh.objstorage.exc import Error, ObjNotFoundError
from swh.objstorage.interface import CompositeObjId, ObjId
from swh.objstorage.objstorage import ObjStorage, compute_hash, objid_to_default_hex


class InMemoryObjStorage(ObjStorage):
    """In-Memory objstorage.

    Intended for test purposes.

    """

    def __init__(self, **args):
        super().__init__()
        self.state = {}

    def check_config(self, *, check_write):
        return True

    def __contains__(self, obj_id: ObjId) -> bool:
        return obj_id in self.state

    def __iter__(self) -> Iterator[CompositeObjId]:
        return iter(sorted(self.state))

    def add(self, content: bytes, obj_id: ObjId, check_presence: bool = True) -> None:
        if check_presence and obj_id in self:
            return

        self.state[obj_id] = content

    def get(self, obj_id: ObjId) -> bytes:
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)

        return self.state[obj_id]

    def check(self, obj_id: ObjId) -> None:
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)
        if compute_hash(self.state[obj_id]) != obj_id:
            raise Error("Corrupt object %s" % objid_to_default_hex(obj_id))

    def delete(self, obj_id: ObjId):
        super().delete(obj_id)  # Check delete permission
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)

        self.state.pop(obj_id)
        return True
