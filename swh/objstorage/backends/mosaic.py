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

from typing import Iterable, Iterator, Tuple

from swh.model.hashutil import HashDict
from swh.mosaic import IdxDescription, MosaicReader
from swh.objstorage.constants import LiteralPrimaryHash
from swh.objstorage.exc import ObjNotFoundError, ReadOnlyObjStorageError
from swh.objstorage.interface import ObjId
from swh.objstorage.metrics import timed
from swh.objstorage.objstorage import ObjStorage

HashToIDX: dict[LiteralPrimaryHash, IdxDescription] = {
    "sha1": IdxDescription.SHA1FMPHGO,
    "sha1_git": IdxDescription.SHA1GITFMPHGO,
    "sha256": IdxDescription.SHA256FMPHGO,
}


class MosaicObjStorage(ObjStorage):
    """Readonly objstorage backed by a single MOSAIC file."""

    name: str = "mosaic"

    def __init__(
        self, path: str, primary_hash: LiteralPrimaryHash = "sha1_git", **kwargs
    ):
        super().__init__(**kwargs)
        self.mosaic_path = path
        self.primary_hash: LiteralPrimaryHash = primary_hash
        self.mosaic = MosaicReader(path, HashToIDX[primary_hash])

    def __del__(self):
        self.mosaic.close()

    def check_config(self, *, check_write):
        return not check_write

    @timed
    def __contains__(self, obj_id: ObjId) -> bool:
        return obj_id[self.primary_hash] in self.mosaic

    @timed
    def add(self, content: bytes, obj_id: ObjId, check_presence: bool = True) -> None:
        raise ReadOnlyObjStorageError("add")

    @timed
    def get(self, obj_id: ObjId) -> bytes:
        try:
            key = obj_id[self.primary_hash]
            return self.mosaic.lookup(key)
        except KeyError:
            raise ObjNotFoundError(obj_id)

    def get_batch(
        self, obj_ids: Iterable[HashDict]
    ) -> Iterator[Tuple[HashDict, bytes]]:
        keys = [obj_id[self.primary_hash] for obj_id in obj_ids]
        for obj_id, content in zip(obj_ids, self.mosaic.get_batch(keys)):
            if content is not None:
                yield (obj_id, content)

    def delete(self, obj_id: ObjId):
        raise ReadOnlyObjStorageError("delete")
