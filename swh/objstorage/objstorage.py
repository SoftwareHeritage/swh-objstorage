# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import bz2
from itertools import dropwhile, islice
import lzma
from typing import Dict
import zlib

from swh.model import hashutil

from .exc import ObjNotFoundError

ID_HASH_ALGO = "sha1"

ID_HEXDIGEST_LENGTH = 40
"""Size in bytes of the hash hexadecimal representation."""

ID_DIGEST_LENGTH = 20
"""Size in bytes of the hash"""

DEFAULT_CHUNK_SIZE = 2 * 1024 * 1024
"""Size in bytes of the streaming chunks"""

DEFAULT_LIMIT = 10000
"""Default number of results of ``list_content``."""


def compute_hash(content):
    """Compute the content's hash.

    Args:
        content (bytes): The raw content to hash
        hash_name (str): Hash's name (default to ID_HASH_ALGO)

    Returns:
        The ID_HASH_ALGO for the content

    """
    return (
        hashutil.MultiHash.from_data(content, hash_names=[ID_HASH_ALGO],)
        .digest()
        .get(ID_HASH_ALGO)
    )


class NullCompressor:
    def compress(self, data):
        return data

    def flush(self):
        return b""


class NullDecompressor:
    def decompress(self, data):
        return data

    @property
    def unused_data(self):
        return b""


decompressors = {
    "bz2": bz2.BZ2Decompressor,
    "lzma": lzma.LZMADecompressor,
    "gzip": lambda: zlib.decompressobj(wbits=31),
    "zlib": zlib.decompressobj,
    "none": NullDecompressor,
}

compressors = {
    "bz2": bz2.BZ2Compressor,
    "lzma": lzma.LZMACompressor,
    "gzip": lambda: zlib.compressobj(wbits=31),
    "zlib": zlib.compressobj,
    "none": NullCompressor,
}


class ObjStorage(metaclass=abc.ABCMeta):
    def __init__(self, *, allow_delete=False, **kwargs):
        # A more complete permission system could be used in place of that if
        # it becomes needed
        self.allow_delete = allow_delete

    @abc.abstractmethod
    def check_config(self, *, check_write):
        pass

    @abc.abstractmethod
    def __contains__(self, obj_id):
        pass

    @abc.abstractmethod
    def add(self, content, obj_id=None, check_presence=True):
        pass

    def add_batch(self, contents, check_presence=True) -> Dict:
        summary = {"object:add": 0, "object:add:bytes": 0}
        for obj_id, content in contents.items():
            if check_presence and obj_id in self:
                continue
            self.add(content, obj_id, check_presence=False)
            summary["object:add"] += 1
            summary["object:add:bytes"] += len(content)
        return summary

    def restore(self, content, obj_id=None):
        # check_presence to false will erase the potential previous content.
        return self.add(content, obj_id, check_presence=False)

    @abc.abstractmethod
    def get(self, obj_id):
        pass

    def get_batch(self, obj_ids):
        for obj_id in obj_ids:
            try:
                yield self.get(obj_id)
            except ObjNotFoundError:
                yield None

    @abc.abstractmethod
    def check(self, obj_id):
        pass

    @abc.abstractmethod
    def delete(self, obj_id):
        if not self.allow_delete:
            raise PermissionError("Delete is not allowed.")

    # Management methods

    def get_random(self, batch_size):
        pass

    # Streaming methods

    def add_stream(self, content_iter, obj_id, check_presence=True):
        raise NotImplementedError

    def get_stream(self, obj_id, chunk_size=DEFAULT_CHUNK_SIZE):
        raise NotImplementedError

    def list_content(self, last_obj_id=None, limit=DEFAULT_LIMIT):
        it = iter(self)
        if last_obj_id:
            it = dropwhile(lambda x: x <= last_obj_id, it)
        return islice(it, limit)
