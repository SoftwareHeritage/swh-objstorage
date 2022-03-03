# Copyright (C) 2015-2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict

from typing_extensions import Protocol, runtime_checkable

from swh.core.api import remote_api_endpoint
from swh.objstorage.objstorage import DEFAULT_CHUNK_SIZE, DEFAULT_LIMIT


@runtime_checkable
class ObjStorageInterface(Protocol):
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

    - get_stream()     same as get() but returns a chunked iterator

    Each implementation of this interface can have a different behavior and
    its own way to store the contents.
    """

    @remote_api_endpoint("check_config")
    def check_config(self, *, check_write):
        """Check whether the object storage is properly configured.

        Args:
            check_write (bool): if True, check if writes to the object storage
            can succeed.

        Returns:
            True if the configuration check worked, an exception if it didn't.
        """
        ...

    @remote_api_endpoint("content/contains")
    def __contains__(self, obj_id):
        """Indicate if the given object is present in the storage.

        Args:
            obj_id (bytes): object identifier.

        Returns:
            True if and only if the object is present in the current object
            storage.

        """
        ...

    @remote_api_endpoint("content/add")
    def add(self, content, obj_id=None, check_presence=True):
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
        ...

    @remote_api_endpoint("content/add/batch")
    def add_batch(self, contents, check_presence=True) -> Dict:
        """Add a batch of new objects to the object storage.

        Args:
            contents: mapping from obj_id to object contents

        Returns:
            the summary of objects added to the storage (count of object,
            count of bytes object)

        """
        ...

    def restore(self, content, obj_id=None):
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
        ...

    @remote_api_endpoint("content/get")
    def get(self, obj_id):
        """Retrieve the content of a given object.

        Args:
            obj_id (bytes): object id.

        Returns:
            the content of the requested object as bytes.

        Raises:
            ObjNotFoundError: if the requested object is missing.

        """
        ...

    @remote_api_endpoint("content/get/batch")
    def get_batch(self, obj_ids):
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
        ...

    @remote_api_endpoint("content/check")
    def check(self, obj_id):
        """Perform an integrity check for a given object.

        Verify that the file object is in place and that the content matches
        the object id.

        Args:
            obj_id (bytes): object identifier.

        Raises:
            ObjNotFoundError: if the requested object is missing.
            Error: if the request object is corrupted.

        """
        ...

    @remote_api_endpoint("content/delete")
    def delete(self, obj_id):
        """Delete an object.

        Args:
            obj_id (bytes): object identifier.

        Raises:
            ObjNotFoundError: if the requested object is missing.

        """
        ...

    # Management methods

    @remote_api_endpoint("content/get/random")
    def get_random(self, batch_size):
        """Get random ids of existing contents.

        This method is used in order to get random ids to perform
        content integrity verifications on random contents.

        Args:
            batch_size (int): Number of ids that will be given

        Yields:
            An iterable of ids (bytes) of contents that are in the
            current object storage.

        """
        ...

    # Streaming methods

    def get_stream(self, obj_id, chunk_size=DEFAULT_CHUNK_SIZE):
        """Retrieve the content of a given object as a chunked iterator.

        Args:
            obj_id (bytes): object id.

        Returns:
            the content of the requested object as bytes.

        Raises:
            ObjNotFoundError: if the requested object is missing.

        """
        ...

    def __iter__(self):
        ...

    def list_content(self, last_obj_id=None, limit=DEFAULT_LIMIT):
        """Generates known object ids.

        Args:
            last_obj_id (bytes): object id from which to iterate from
                 (excluded).
            limit (int): max number of object ids to generate.

        Generates:
            obj_id (bytes): object ids.
        """
        ...
