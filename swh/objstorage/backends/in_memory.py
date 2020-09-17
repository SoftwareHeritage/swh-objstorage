# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import io

from swh.objstorage.exc import Error, ObjNotFoundError
from swh.objstorage.objstorage import DEFAULT_CHUNK_SIZE, ObjStorage, compute_hash


class InMemoryObjStorage(ObjStorage):
    """In-Memory objstorage.

    Intended for test purposes.

    """

    def __init__(self, **args):
        super().__init__()
        self.state = {}

    def check_config(self, *, check_write):
        return True

    def __contains__(self, obj_id, *args, **kwargs):
        return obj_id in self.state

    def __iter__(self):
        return iter(sorted(self.state))

    def add(self, content, obj_id=None, check_presence=True, *args, **kwargs):
        if obj_id is None:
            obj_id = compute_hash(content)

        if check_presence and obj_id in self:
            return obj_id

        self.state[obj_id] = content

        return obj_id

    def get(self, obj_id, *args, **kwargs):
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)

        return self.state[obj_id]

    def check(self, obj_id, *args, **kwargs):
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)
        if compute_hash(self.state[obj_id]) != obj_id:
            raise Error("Corrupt object %s" % obj_id)
        return True

    def delete(self, obj_id, *args, **kwargs):
        super().delete(obj_id)  # Check delete permission
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)

        self.state.pop(obj_id)
        return True

    def get_stream(self, obj_id, chunk_size=DEFAULT_CHUNK_SIZE):
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)

        data = io.BytesIO(self.state[obj_id])
        reader = functools.partial(data.read, chunk_size)
        yield from iter(reader, b"")
