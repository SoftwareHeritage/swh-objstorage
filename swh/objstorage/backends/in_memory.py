# Copyright (C) 2017-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict

from swh.objstorage.constants import LiteralPrimaryHash
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.interface import ObjId
from swh.objstorage.objstorage import ObjStorage, timed


class InMemoryObjStorage(ObjStorage):
    """In-Memory objstorage.

    Intended for test purposes.

    """

    primary_hash: LiteralPrimaryHash = "sha1"
    name: str = "memory"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.state: Dict[bytes, bytes] = {}

    def check_config(self, *, check_write):
        return True

    def _state_key(self, obj_id: ObjId) -> bytes:
        return obj_id[self.primary_hash]

    @timed
    def __contains__(self, obj_id: ObjId) -> bool:
        return self._state_key(obj_id) in self.state

    @timed
    def add(self, content: bytes, obj_id: ObjId, check_presence: bool = True) -> None:
        if check_presence and obj_id in self:
            return

        self.state[self._state_key(obj_id)] = content

    @timed
    def get(self, obj_id: ObjId) -> bytes:
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)

        return self.state[self._state_key(obj_id)]

    def delete(self, obj_id: ObjId):
        super().delete(obj_id)  # Check delete permission
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)

        self.state.pop(self._state_key(obj_id))
        return True
