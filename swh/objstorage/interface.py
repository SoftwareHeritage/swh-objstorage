# Copyright (C) 2015-2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, Iterator, List, Optional, Tuple, Union

from typing_extensions import Protocol, TypedDict, runtime_checkable

from swh.core.api import remote_api_endpoint
from swh.model.model import Sha1
from swh.objstorage.constants import DEFAULT_LIMIT


class CompositeObjId(TypedDict, total=False):
    sha1: bytes
    sha1_git: bytes
    sha256: bytes
    blake2s256: bytes


ObjId = Union[bytes, CompositeObjId]
"""Type of object ids, which should be ``{hash: value for hash in SUPPORTED_HASHES}``;
but single sha1 hashes are supported for legacy clients"""


@runtime_checkable
class ObjStorageInterface(Protocol):
    """High-level API to manipulate the Software Heritage object storage.

    Conceptually, the object storage offers the following methods:

    - check_config()  check if the object storage is properly configured
    - __contains__()  check if an object is present, by object id
    - add()           add a new object, returning an object id
    - restore()       same as add() but erase an already existed content
    - get()           retrieve the content of an object, by object id
    - check()         check the integrity of an object, by object id
    - delete()        remove an object

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
    def __contains__(self, obj_id: ObjId) -> bool:
        """Indicate if the given object is present in the storage.

        Args:
            obj_id: object identifier.

        Returns:
            True if and only if the object is present in the current object
            storage.

        """
        ...

    @remote_api_endpoint("content/add")
    def add(self, content: bytes, obj_id: ObjId, check_presence: bool = True) -> None:
        """Add a new object to the object storage.

        Args:
            content: object's raw content to add in storage.
            obj_id: either dict of checksums, or single checksum of
                [bytes] using [ID_HASH_ALGO] algorithm.
                It is trusted to match the bytes.
            check_presence (bool): indicate if the presence of the
                content should be verified before adding the file.

        Returns:
            the id (bytes) of the object into the storage.

        """
        ...

    @remote_api_endpoint("content/add/batch")
    def add_batch(
        self,
        contents: Union[Dict[Sha1, bytes], List[Tuple[ObjId, bytes]]],
        check_presence: bool = True,
    ) -> Dict:
        """Add a batch of new objects to the object storage.

        Args:
            contents: either mapping from [ID_HASH_ALGO] checksums to object contents,
                or list of pairs of dict hashes and object contents

        Returns:
            the summary of objects added to the storage (count of object,
            count of bytes object)

        """
        ...

    def restore(self, content: bytes, obj_id: ObjId) -> None:
        """Restore a content that have been corrupted.

        This function is identical to add but does not check if
        the object id is already in the file system.
        The default implementation provided by the current class is
        suitable for most cases.

        Args:
            content: object's raw content to add in storage
            obj_id: dict of hashes of the content (or only the sha1, for legacy clients)
        """
        ...

    @remote_api_endpoint("content/get")
    def get(self, obj_id: ObjId) -> bytes:
        """Retrieve the content of a given object.

        Args:
            obj_id: object id.

        Returns:
            the content of the requested object as bytes.

        Raises:
            ObjNotFoundError: if the requested object is missing.

        """
        ...

    @remote_api_endpoint("content/get/batch")
    def get_batch(self, obj_ids: List[ObjId]) -> Iterator[Optional[bytes]]:
        """Retrieve objects' raw content in bulk from storage.

        Note: This function does have a default implementation in
        ObjStorage that is suitable for most cases.

        For object storages that needs to do the minimal number of
        requests possible (ex: remote object storages), that method
        can be overridden to perform a more efficient operation.

        Args:
            obj_ids: list of object ids.

        Returns:
            list of resulting contents, or None if the content could
            not be retrieved. Do not raise any exception as a fail for
            one content will not cancel the whole request.

        """
        ...

    @remote_api_endpoint("content/check")
    def check(self, obj_id: ObjId) -> None:
        """Perform an integrity check for a given object.

        Verify that the file object is in place and that the content matches
        the object id.

        Args:
            obj_id: object identifier.

        Raises:
            ObjNotFoundError: if the requested object is missing.
            Error: if the request object is corrupted.

        """
        ...

    @remote_api_endpoint("content/delete")
    def delete(self, obj_id: ObjId):
        """Delete an object.

        Args:
            obj_id: object identifier.

        Raises:
            ObjNotFoundError: if the requested object is missing.

        """
        ...

    def __iter__(self) -> Iterator[CompositeObjId]:
        ...

    def list_content(
        self, last_obj_id: Optional[ObjId] = None, limit: Optional[int] = DEFAULT_LIMIT
    ) -> Iterator[CompositeObjId]:
        """Generates known object ids.

        Args:
            last_obj_id: object id from which to iterate from
                 (excluded).
            limit (int): max number of object ids to generate. If unset (None),
                 generate all objects (behavior might not be guaranteed for all
                 backends).

        Generates:
            obj_id: object ids.

        """
        ...
