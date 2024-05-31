# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import bz2
import collections
from datetime import timedelta
import functools
from itertools import dropwhile, islice
import lzma
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    Literal,
    Mapping,
    Optional,
    Tuple,
    Union,
)
import zlib

from typing_extensions import Protocol

from swh.core import statsd
from swh.model import hashutil
from swh.model.model import Sha1
from swh.objstorage.constants import DEFAULT_LIMIT, ID_HASH_ALGO
from swh.objstorage.exc import ObjCorruptedError, ObjNotFoundError
from swh.objstorage.interface import CompositeObjId, ObjId, ObjStorageInterface

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


def objid_to_default_hex(
    obj_id: ObjId, algo: Literal["sha1", "sha256"] = ID_HASH_ALGO
) -> str:
    """Converts SHA1 hashes and multi-hashes to the hexadecimal representation
    of the SHA1."""
    if isinstance(obj_id, bytes):
        return hashutil.hash_to_hex(obj_id)
    elif isinstance(obj_id, str):
        return obj_id
    else:
        return hashutil.hash_to_hex(obj_id[algo])


def compute_hashes(
    content: bytes, hash_names: Iterable[str] = hashutil.DEFAULT_ALGORITHMS
) -> Dict[str, bytes]:
    """Compute the content's hashes.

    Args:
        content: The raw content to hash
        hash_names: Names of hashing algorithms
            (default to :const:`swh.model.hashutil.DEFAULT_ALGORITHMS`)

    Returns:
        A dict mapping algo name to hash value

    """
    return hashutil.MultiHash.from_data(
        content,
        hash_names=hash_names,
    ).digest()


def compute_hash(content: bytes, algo: str = ID_HASH_ALGO) -> bytes:
    """Compute the content's hash.

    Args:
        content: The raw content to hash
        hash_name: Hash's name

    Returns:
        The computed hash for the content

    """
    return compute_hashes(content, [algo])[algo]


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
    def compress(self, data: bytes) -> bytes:
        ...

    def flush(self) -> bytes:
        ...


class _DecompressorProtocol(Protocol):
    def decompress(self, data: bytes) -> bytes:
        ...

    unused_data: bytes


decompressors: Dict[str, Callable[[], _DecompressorProtocol]] = {
    "bz2": bz2.BZ2Decompressor,  # type: ignore
    "lzma": lzma.LZMADecompressor,  # type: ignore
    "gzip": lambda: zlib.decompressobj(wbits=31),
    "zlib": zlib.decompressobj,
    "none": NullDecompressor,  # type: ignore
}

compressors: Dict[str, Callable[[], _CompressorProtocol]] = {
    "bz2": bz2.BZ2Compressor,
    "lzma": lzma.LZMACompressor,
    "gzip": lambda: zlib.compressobj(wbits=31),
    "zlib": zlib.compressobj,
    "none": NullCompressor,
}

CompressionFormat = Literal["bz2", "lzma", "gzip", "zlib", "none"]


class ObjStorage(metaclass=abc.ABCMeta):
    PRIMARY_HASH: Literal["sha1", "sha256"] = "sha1"
    compression: CompressionFormat = "none"
    name: str = "objstorage"
    """Default objstorage name; can be overloaded at instantiation time giving a
    'name' argument to the constructor"""

    def __init__(
        self: ObjStorageInterface,
        *,
        allow_delete: bool = False,
        check_write: bool = True,
        **kwargs,
    ):
        # A more complete permission system could be used in place of that if
        # it becomes needed
        self.allow_delete = allow_delete
        self.check_write = check_write
        # if no name is given in kwargs, default to name defined as class attribute
        if "name" in kwargs:
            self.name = kwargs["name"]

    def add_batch(
        self: ObjStorageInterface,
        contents: Union[Mapping[Sha1, bytes], Iterable[Tuple[ObjId, bytes]]],
        check_presence: bool = True,
    ) -> Dict:
        summary = {"object:add": 0, "object:add:bytes": 0}
        contents_pairs: Iterable[Tuple[ObjId, bytes]]
        if isinstance(contents, collections.abc.Mapping):
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
        self: ObjStorageInterface, obj_ids: Iterable[ObjId]
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

    def download_url(
        self,
        obj_id: ObjId,
        content_disposition: Optional[str] = None,
        expiry: Optional[timedelta] = None,
    ) -> Optional[str]:
        return None

    @abc.abstractmethod
    def get(self, obj_id: ObjId) -> bytes:
        raise NotImplementedError()

    def check(self, obj_id: ObjId) -> None:
        """Check if a content is found and recompute its hash to check integrity."""
        obj_content = self.get(obj_id)
        hash_algos = [str(self.PRIMARY_HASH)]
        if isinstance(obj_id, dict):
            hash_algos += [algo for algo in obj_id if algo != self.PRIMARY_HASH]
        actual_hashes = compute_hashes(obj_content, hash_algos)
        for algo in hash_algos:
            actual_obj_id = actual_hashes[algo]
            expected_obj_id = obj_id
            if isinstance(obj_id, dict):
                expected_obj_id = obj_id[algo]  # type: ignore[literal-required]
            if actual_obj_id != expected_obj_id:
                raise ObjCorruptedError(
                    f"expected {algo} hash is {hashutil.hash_to_hex(expected_obj_id)}, "
                    f"actual {algo} hash is {hashutil.hash_to_hex(actual_obj_id)}"
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
                f"content with {self.PRIMARY_HASH} hash {hex_obj_id} is not a proper "
                "compressed file"
            )
        if decompressor.unused_data:
            raise ObjCorruptedError(
                f"trailing data found when decompressing content with {self.PRIMARY_HASH} "
                f"{hex_obj_id}"
            )
        return ret
