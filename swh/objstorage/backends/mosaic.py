# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
Implementation of the ObjStorage API based on a single MOSAIC file.

This is suitable for small objects collections (less than 100 million objects) that
can benefit from the efficiency of :ref:`swh-mosaic` without setting up a complete
:ref:`swh-objstorage-winery`.
"""

from swh.mosaic import IdxDescription, MosaicReader
from swh.objstorage.constants import ID_HEXDIGEST_LENGTH_BY_ALGO, LiteralPrimaryHash
from swh.objstorage.exc import ObjNotFoundError, ReadOnlyObjStorageError
from swh.objstorage.interface import ObjId
from swh.objstorage.objstorage import CompressionFormat, ObjStorage, timed


class MosaicObjStorage(ObjStorage):
    """Readonly objstorage backed by a single MOSAIC file."""

    primary_hash: LiteralPrimaryHash = "sha1_git"
    name: str = "mosaic"

    def __init__(self, path: str, compression: CompressionFormat, **kwargs):
        super().__init__(**kwargs)
        self.mosaic_path = path
        self.mosaic = MosaicReader(path, IdxDescription.SHA1GITFMPHGO)
        self.key_len = 20
        hash_len = ID_HEXDIGEST_LENGTH_BY_ALGO[self.primary_hash] // 2
        assert self.key_len >= hash_len

    def __del__(self):
        self.mosaic.close()

    def check_config(self, *, check_write):
        return not check_write

    @timed
    def __contains__(self, obj_id: ObjId) -> bool:
        key = obj_id[self.primary_hash].rjust(self.key_len, b"\0")
        res = True
        try:
            self.mosaic.lookup(key)
        except KeyError:
            res = False
        return res

    @timed
    def add(self, content: bytes, obj_id: ObjId, check_presence: bool = True) -> None:
        raise ReadOnlyObjStorageError("add")

    @timed
    def get(self, obj_id: ObjId) -> bytes:
        try:
            key = obj_id[self.primary_hash].rjust(self.key_len, b"\0")
            return self.mosaic.lookup(key)
        except KeyError:
            raise ObjNotFoundError(obj_id)

    def delete(self, obj_id: ObjId):
        raise ReadOnlyObjStorageError("delete")
