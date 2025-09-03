# Copyright (C) 2015-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import bz2
from datetime import timedelta
import functools
import lzma
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    Literal,
    Optional,
    Protocol,
    Tuple,
)
import zlib

from swh.core import statsd
from swh.model.hashutil import HashDict, MultiHash, hash_to_hex
from swh.objstorage.constants import LiteralPrimaryHash
from swh.objstorage.exc import ObjCorruptedError, ObjNotFoundError
from swh.objstorage.interface import ObjStorageInterface, objid_from_dict

DURATION_METRICS = "swh_objstorage_request_duration_seconds"


def timed(f):
    """A simple decorator used to add statsd probes on main ObjStorage methods
    (add, get and __contains__)
    """

    @functools.wraps(f)
    def w(self, *a, **kw):
        with statsd.statsd.timed(
            DURATION_METRICS,
            tags={"endpoint": f.__name__, "name": self.name},
        ):
            return f(self, *a, **kw)

    w._timed = True
    w._f = f
    return w


def objid_to_default_hex(obj_id: HashDict, algo: LiteralPrimaryHash) -> str:
    """Converts multi-hashes to the hexadecimal representation
    of the given hash algo."""
    return hash_to_hex(obj_id[algo])


def objid_for_content(content: bytes) -> HashDict:
    """Compute the content's hashes.

    Args:
        content: The raw content to hash
        hash_names: Names of hashing algorithms
            (default to :const:`swh.model.hashutil.DEFAULT_ALGORITHMS`)

    Returns:
        A dict mapping algo name to hash value

    """
    return objid_from_dict(
        MultiHash.from_data(
            content,
        ).digest()
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


class _CompressorProtocol(Protocol):
    def compress(self, data: bytes) -> bytes: ...

    def flush(self) -> bytes: ...


class _DecompressorProtocol(Protocol):
    def decompress(self, data: bytes) -> bytes: ...

    @property
    def unused_data(self) -> bytes: ...


decompressors: Dict[str, Callable[[], _DecompressorProtocol]] = {
    "bz2": bz2.BZ2Decompressor,
    "lzma": lzma.LZMADecompressor,
    "gzip": lambda: zlib.decompressobj(wbits=31),
    "zlib": zlib.decompressobj,
    "none": NullDecompressor,
}

compressors: Dict[str, Callable[[], _CompressorProtocol]] = {
    "bz2": bz2.BZ2Compressor,
    "lzma": lzma.LZMACompressor,
    "gzip": lambda: zlib.compressobj(wbits=31),
    "zlib": zlib.compressobj,
    "none": NullCompressor,
}

CompressionFormat = Literal["bz2", "lzma", "gzip", "zlib", "none"]


class ObjStorage(ObjStorageInterface, metaclass=abc.ABCMeta):
    primary_hash: Optional[LiteralPrimaryHash] = None
    compression: CompressionFormat = "none"
    name: str = "objstorage"
    """Default objstorage name; can be overloaded at instantiation time giving a
    'name' argument to the constructor"""

    def __init__(
        self: ObjStorageInterface,
        *,
        allow_delete: bool = False,
        primary_hash: Optional[LiteralPrimaryHash] = None,
        **kwargs,
    ):
        # A more complete permission system could be used in place of that if
        # it becomes needed
        self.allow_delete = allow_delete
        if primary_hash is not None:
            self.primary_hash = primary_hash
        # if no name is given in kwargs, default to name defined as class attribute
        if "name" in kwargs:
            self.name = kwargs["name"]

    def add_batch(
        self: ObjStorageInterface,
        contents: Iterable[Tuple[HashDict, bytes]],
        check_presence: bool = True,
    ) -> Dict:
        summary = {"object:add": 0, "object:add:bytes": 0}
        for obj_id, content in contents:
            if check_presence and obj_id in self:
                continue
            self.add(content, obj_id, check_presence=False)
            summary["object:add"] += 1
            summary["object:add:bytes"] += len(content)
        return summary

    def restore(self: ObjStorageInterface, content: bytes, obj_id: HashDict) -> None:
        # check_presence to false will erase the potential previous content.
        self.add(content, obj_id, check_presence=False)

    def get_batch(
        self: ObjStorageInterface, obj_ids: Iterable[HashDict]
    ) -> Iterator[Optional[bytes]]:
        for obj_id in obj_ids:
            try:
                yield self.get(obj_id)
            except ObjNotFoundError:
                yield None

    @abc.abstractmethod
    def delete(self, obj_id: HashDict):
        if not self.allow_delete:
            raise PermissionError("Delete is not allowed.")

    def download_url(
        self,
        obj_id: HashDict,
        content_disposition: Optional[str] = None,
        expiry: Optional[timedelta] = None,
    ) -> Optional[str]:
        return None

    @abc.abstractmethod
    def get(self, obj_id: HashDict) -> bytes:
        raise NotImplementedError()

    def check(self, obj_id: HashDict) -> None:
        """Check if a content is found and recompute its hash to check integrity."""
        obj_data = self.get(obj_id)
        data_hashes = objid_for_content(obj_data)
        for algo, expected_hash in obj_id.items():
            data_hash = data_hashes[algo]  # type: ignore[literal-required]
            if data_hash != expected_hash:
                raise ObjCorruptedError(
                    f"expected {algo} hash is {hash_to_hex(expected_hash)}, "
                    f"data {algo} hash is {hash_to_hex(data_hash)}"
                )

    def compress(self, data: bytes) -> bytes:
        compressor = compressors[self.compression]()
        compressed = compressor.compress(data)
        compressed += compressor.flush()
        return compressed

    def decompress(self, data: bytes, hex_obj_id: str) -> bytes:
        decompressor = decompressors[self.compression]()
        try:
            ret = decompressor.decompress(data)
        except (zlib.error, lzma.LZMAError, OSError):
            raise ObjCorruptedError(
                f"content with {self.primary_hash} hash {hex_obj_id} is not a proper "
                "compressed file"
            )
        if decompressor.unused_data:
            raise ObjCorruptedError(
                f"trailing data found when decompressing content with {self.primary_hash} "
                f"{hex_obj_id}"
            )
        return ret
