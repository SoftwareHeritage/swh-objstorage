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
ID_HASH_LENGTH = 40  # Size in bytes of the hash hexadecimal representation.
DEFAULT_CHUNK_SIZE = 2 * 1024 * 1024  # Size in bytes of the streaming chunks
DEFAULT_LIMIT = 10000


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
    """ High-level API to manipulate the Software Heritage object storage.

    Conceptually, the object storage offers the following methods:

    - check_config()  check if the object storage is properly configured
    - __contains__()  check if an object is present, by object id
    - add()           add a new object, returning an object id
    - restore()       same as add() but erase an already existed content
    - get()           retrieve the content of an object, by object id
    - check()         check the integrity of an object, by object id
    - delete()        remove an object

    And some management methods:

    - get_random()    get random object id of existing contents (used for the
                      content integrity checker).

    Some of the methods have available streaming equivalents:

    - add_stream()     same as add() but with a chunked iterator
    - restore_stream() same as add_stream() but erase already existing content
    - get_stream()     same as get() but returns a chunked iterator

    Each implementation of this interface can have a different behavior and
    its own way to store the contents.
    """

    def __init__(self, *, allow_delete=False, **kwargs):
        # A more complete permission system could be used in place of that if
        # it becomes needed
        self.allow_delete = allow_delete

    @abc.abstractmethod
    def check_config(self, *, check_write):
        """Check whether the object storage is properly configured.

        Args:
            check_write (bool): if True, check if writes to the object storage
            can succeed.

        Returns:
            True if the configuration check worked, an exception if it didn't.
        """
        pass

    @abc.abstractmethod
    def __contains__(self, obj_id, *args, **kwargs):
        """Indicate if the given object is present in the storage.

        Args:
            obj_id (bytes): object identifier.

        Returns:
            True if and only if the object is present in the current object
            storage.

        """
        pass

    @abc.abstractmethod
    def add(self, content, obj_id=None, check_presence=True, *args, **kwargs):
        """Add a new object to the object storage.

        Args:
            content (bytes): object's raw content to add in storage.
            obj_id (bytes): checksum of [bytes] using [ID_HASH_ALGO]
                algorithm. When given, obj_id will be trusted to match
                the bytes. If missing, obj_id will be computed on the
                fly.
            check_presence (bool): indicate if the presence of the
                content should be verified before adding the file.

        Returns:
            the id (bytes) of the object into the storage.

        """
        pass

    def add_batch(self, contents, check_presence=True) -> Dict:
        """Add a batch of new objects to the object storage.

        Args:
            contents: mapping from obj_id to object contents

        Returns:
            the summary of objects added to the storage (count of object,
            count of bytes object)

        """
        summary = {"object:add": 0, "object:add:bytes": 0}
        for obj_id, content in contents.items():
            if check_presence and obj_id in self:
                continue
            self.add(content, obj_id, check_presence=False)
            summary["object:add"] += 1
            summary["object:add:bytes"] += len(content)
        return summary

    def restore(self, content, obj_id=None, *args, **kwargs):
        """Restore a content that have been corrupted.

        This function is identical to add but does not check if
        the object id is already in the file system.
        The default implementation provided by the current class is
        suitable for most cases.

        Args:
            content (bytes): object's raw content to add in storage
            obj_id (bytes): checksum of `bytes` as computed by
                ID_HASH_ALGO. When given, obj_id will be trusted to
                match bytes. If missing, obj_id will be computed on
                the fly.

        """
        # check_presence to false will erase the potential previous content.
        return self.add(content, obj_id, check_presence=False)

    @abc.abstractmethod
    def get(self, obj_id, *args, **kwargs):
        """Retrieve the content of a given object.

        Args:
            obj_id (bytes): object id.

        Returns:
            the content of the requested object as bytes.

        Raises:
            ObjNotFoundError: if the requested object is missing.

        """
        pass

    def get_batch(self, obj_ids, *args, **kwargs):
        """Retrieve objects' raw content in bulk from storage.

        Note: This function does have a default implementation in
        ObjStorage that is suitable for most cases.

        For object storages that needs to do the minimal number of
        requests possible (ex: remote object storages), that method
        can be overridden to perform a more efficient operation.

        Args:
            obj_ids ([bytes]: list of object ids.

        Returns:
            list of resulting contents, or None if the content could
            not be retrieved. Do not raise any exception as a fail for
            one content will not cancel the whole request.

        """
        for obj_id in obj_ids:
            try:
                yield self.get(obj_id)
            except ObjNotFoundError:
                yield None

    @abc.abstractmethod
    def check(self, obj_id, *args, **kwargs):
        """Perform an integrity check for a given object.

        Verify that the file object is in place and that the content matches
        the object id.

        Args:
            obj_id (bytes): object identifier.

        Raises:
            ObjNotFoundError: if the requested object is missing.
            Error: if the request object is corrupted.

        """
        pass

    @abc.abstractmethod
    def delete(self, obj_id, *args, **kwargs):
        """Delete an object.

        Args:
            obj_id (bytes): object identifier.

        Raises:
            ObjNotFoundError: if the requested object is missing.

        """
        if not self.allow_delete:
            raise PermissionError("Delete is not allowed.")

    # Management methods

    def get_random(self, batch_size, *args, **kwargs):
        """Get random ids of existing contents.

        This method is used in order to get random ids to perform
        content integrity verifications on random contents.

        Args:
            batch_size (int): Number of ids that will be given

        Yields:
            An iterable of ids (bytes) of contents that are in the
            current object storage.

        """
        pass

    # Streaming methods

    def add_stream(self, content_iter, obj_id, check_presence=True):
        """Add a new object to the object storage using streaming.

        This function is identical to add() except it takes a generator that
        yields the chunked content instead of the whole content at once.

        Args:
            content (bytes): chunked generator that yields the object's raw
                content to add in storage.
            obj_id (bytes): object identifier
            check_presence (bool): indicate if the presence of the
                content should be verified before adding the file.

        Returns:
            the id (bytes) of the object into the storage.

        """
        raise NotImplementedError

    def restore_stream(self, content_iter, obj_id=None):
        """Restore a content that have been corrupted using streaming.

        This function is identical to restore() except it takes a generator
        that yields the chunked content instead of the whole content at once.
        The default implementation provided by the current class is
        suitable for most cases.

        Args:
            content (bytes): chunked generator that yields the object's raw
                content to add in storage.
            obj_id (bytes): object identifier

        """
        # check_presence to false will erase the potential previous content.
        return self.add_stream(content_iter, obj_id, check_presence=False)

    def get_stream(self, obj_id, chunk_size=DEFAULT_CHUNK_SIZE):
        """Retrieve the content of a given object as a chunked iterator.

        Args:
            obj_id (bytes): object id.

        Returns:
            the content of the requested object as bytes.

        Raises:
            ObjNotFoundError: if the requested object is missing.

        """
        raise NotImplementedError

    def list_content(self, last_obj_id=None, limit=DEFAULT_LIMIT):
        """Generates known object ids.

        Args:
            last_obj_id (bytes): object id from which to iterate from
                 (excluded).
            limit (int): max number of object ids to generate.

        Generates:
            obj_id (bytes): object ids.
        """
        it = iter(self)
        if last_obj_id:
            it = dropwhile(lambda x: x <= last_obj_id, it)
        return islice(it, limit)
