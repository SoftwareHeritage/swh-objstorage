# Copyright (C) 2015-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import bz2
from itertools import dropwhile, islice
import lzma
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Tuple, Union
import zlib

from swh.model import hashutil
from swh.model.model import Sha1

from .constants import DEFAULT_LIMIT, ID_HASH_ALGO
from .exc import ObjNotFoundError
from .interface import CompositeObjId, ObjId, ObjStorageInterface


def objid_to_default_hex(obj_id: ObjId) -> str:
    """Converts SHA1 hashes and multi-hashes to the hexadecimal representation
    of the SHA1."""
    if isinstance(obj_id, bytes):
        return hashutil.hash_to_hex(obj_id)
    elif isinstance(obj_id, str):
        return obj_id
    else:
        return hashutil.hash_to_hex(obj_id[ID_HASH_ALGO])


def compute_hash(content, algo=ID_HASH_ALGO):
    """Compute the content's hash.

    Args:
        content (bytes): The raw content to hash
        hash_name (str): Hash's name (default to ID_HASH_ALGO)

    Returns:
        The ID_HASH_ALGO for the content

    """
    return (
        hashutil.MultiHash.from_data(
            content,
            hash_names=[algo],
        )
        .digest()
        .get(algo)
    )


class NullCompressor:
    def compress(self, data):
        return data

    def flush(self):
        return b""


class NullDecompressor:
    def decompress(self, data: bytes) -> bytes:
        return data

    @property
    def unused_data(self) -> bytes:
        return b""


class _CompressorProtocol:
    def compress(self, data: bytes) -> bytes:
        ...

    def flush(self) -> bytes:
        ...


class _DecompressorProtocol:
    def decompress(self, data: bytes) -> bytes:
        ...

    unused_data: bytes


decompressors: Dict[str, Callable[[], _DecompressorProtocol]] = {
    "bz2": bz2.BZ2Decompressor,  # type: ignore
    "lzma": lzma.LZMADecompressor,  # type: ignore
    "gzip": lambda: zlib.decompressobj(wbits=31),  # type: ignore
    "zlib": zlib.decompressobj,  # type: ignore
    "none": NullDecompressor,  # type: ignore
}

compressors: Dict[str, Callable[[], _CompressorProtocol]] = {
    "bz2": bz2.BZ2Compressor,  # type: ignore
    "lzma": lzma.LZMACompressor,  # type: ignore
    "gzip": lambda: zlib.compressobj(wbits=31),  # type: ignore
    "zlib": zlib.compressobj,  # type: ignore
    "none": NullCompressor,  # type: ignore
}


class ObjStorage(metaclass=abc.ABCMeta):
    def __init__(self, *, allow_delete=False, **kwargs):
        # A more complete permission system could be used in place of that if
        # it becomes needed
        self.allow_delete = allow_delete

    def add_batch(
        self: ObjStorageInterface,
        contents: Union[Dict[Sha1, bytes], List[Tuple[ObjId, bytes]]],
        check_presence: bool = True,
    ) -> Dict:
        summary = {"object:add": 0, "object:add:bytes": 0}
        contents_pairs: Iterable[Tuple[ObjId, bytes]]
        if isinstance(contents, dict):
            contents_pairs = contents.items()
        else:
            contents_pairs = contents
        for obj_id, content in contents_pairs:
            if check_presence and obj_id in self:
                continue
            self.add(content, obj_id, check_presence=False)
            summary["object:add"] += 1
            summary["object:add:bytes"] += len(content)
        return summary

    def restore(self: ObjStorageInterface, content: bytes, obj_id: ObjId) -> None:
        # check_presence to false will erase the potential previous content.
        self.add(content, obj_id, check_presence=False)

    def get_batch(
        self: ObjStorageInterface, obj_ids: List[ObjId]
    ) -> Iterator[Optional[bytes]]:
        for obj_id in obj_ids:
            try:
                yield self.get(obj_id)
            except ObjNotFoundError:
                yield None

    @abc.abstractmethod
    def delete(self, obj_id: ObjId):
        if not self.allow_delete:
            raise PermissionError("Delete is not allowed.")

    def list_content(
        self: ObjStorageInterface,
        last_obj_id: Optional[ObjId] = None,
        limit: Optional[int] = DEFAULT_LIMIT,
    ) -> Iterator[CompositeObjId]:
        it = iter(self)
        if last_obj_id:
            last_obj_id_hex = objid_to_default_hex(last_obj_id)
            it = dropwhile(lambda x: objid_to_default_hex(x) <= last_obj_id_hex, it)
        return islice(it, limit)
