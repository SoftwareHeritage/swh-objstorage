# Copyright (C) 2017-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, Iterator

from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.interface import CompositeObjId, ObjId
from swh.objstorage.objstorage import ObjStorage


class InMemoryObjStorage(ObjStorage):
    """In-Memory objstorage.

    Intended for test purposes.

    """

    PRIMARY_HASH = "sha1"
    name: str = "memory"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.state: Dict[bytes, bytes] = {}

    def check_config(self, *, check_write):
        return True

    def _state_key(self, obj_id: ObjId) -> bytes:
        if isinstance(obj_id, dict):
            return obj_id[self.PRIMARY_HASH]
        else:
            return obj_id

    def __contains__(self, obj_id: ObjId) -> bool:
        return self._state_key(obj_id) in self.state

    def __iter__(self) -> Iterator[CompositeObjId]:
        for id_ in sorted(self.state):
            yield {self.PRIMARY_HASH: id_}

    def add(self, content: bytes, obj_id: ObjId, check_presence: bool = True) -> None:
        if check_presence and obj_id in self:
            return

        self.state[self._state_key(obj_id)] = content

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
