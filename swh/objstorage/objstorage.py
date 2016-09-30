# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc

from swh.core import hashutil

from .exc import ObjNotFoundError


ID_HASH_ALGO = 'sha1'
ID_HASH_LENGTH = 40  # Size in bytes of the hash hexadecimal representation.


def compute_hash(content):
    return hashutil.hashdata(
        content,
        algorithms=[ID_HASH_ALGO]
    ).get(ID_HASH_ALGO)


class ObjStorage(metaclass=abc.ABCMeta):
    """ High-level API to manipulate the Software Heritage object storage.

    Conceptually, the object storage offers 5 methods:

    - __contains__()  check if an object is present, by object id
    - add()           add a new object, returning an object id
    - restore()       same as add() but erase an already existed content
    - get()           retrieve the content of an object, by object id
    - check()         check the integrity of an object, by object id

    And some management methods:

    - get_random()    get random object id of existing contents (used for the
                      content integrity checker).

    Each implementation of this interface can have a different behavior and
    its own way to store the contents.
    """

    @abc.abstractmethod
    def __contains__(self, obj_id, *args, **kwargs):
        """Indicate if the given object is present in the storage.

        Args:
            obj_id (bytes): object identifier.

        Returns:
            True iff the object is present in the current object storage.

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

    def restore(self, content, obj_id=None, *args, **kwargs):
        """Restore a content that have been corrupted.

        This function is identical to add_bytes but does not check if
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
        can be overriden to perform a more efficient operation.

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

        Verify that the file object is in place and that the gziped content
        matches the object id.

        Args:
            obj_id (bytes): object identifier.

        Raises:
            ObjNotFoundError: if the requested object is missing.
            Error: if the request object is corrupted.

        """
        pass

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
