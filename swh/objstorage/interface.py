# Copyright (C) 2015-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from datetime import timedelta
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    Optional,
    Protocol,
    Tuple,
    runtime_checkable,
)

from swh.core.api import remote_api_endpoint
from swh.model.hashutil import DEFAULT_ALGORITHMS as COMPOSITE_OBJID_KEYS
from swh.model.hashutil import HashDict
from swh.objstorage.constants import LiteralPrimaryHash

CompositeObjId = HashDict
ObjId = HashDict


def objid_from_dict(d: Dict[str, Any] | HashDict) -> HashDict:
    """Generate an object id from a dict of optional hashes"""
    filtered: HashDict = {}

    for key in COMPOSITE_OBJID_KEYS:
        value = d.get(key)
        if value is None:
            continue
        if not isinstance(value, bytes):
            raise TypeError(f"value for {key} is {value.__class__.__name__}, not bytes")
        filtered[key] = value

    if not filtered:
        raise ValueError(
            f"dict is missing at least one of {', '.join(COMPOSITE_OBJID_KEYS)}"
        )

    return filtered


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

    and the following attributes:

    - name            name given to the object storage; useful e.g. for logging in
                      composite object storagges (multiplexer)
    - primary_hash    the hash algorithm used by this backend as primary key for
                      content objects. Can be None for object storages that to
                      now implement object storage themselves (e.g. proxy
                      objstorage)

    Each implementation of this interface can have a different behavior and
    its own way to store the contents.

    """

    name: str
    # defined only for actual backends, but not for proxies/rpc etc.
    primary_hash: Optional[LiteralPrimaryHash] = None

    def __init__(
        self,
        *,
        name: str = "",
        **kwargs,
    ): ...

    @remote_api_endpoint("check_config")
    def check_config(self, *, check_write):
        """Check whether the object storage is properly configured.

        Args:
            check_write (bool): if True, check if writes to the object storage
            can succeed.

        Returns:
            True if the configuration check worked, False if 'check_write' is
            True and the object storage is actually read only, and an exception
            if the check failed.

        """
        ...

    @remote_api_endpoint("content/contains")
    def __contains__(self, obj_id: HashDict) -> bool:
        """Indicate if the given object is present in the storage.

        Args:
            obj_id: object identifier.

        Returns:
            True if and only if the object is present in the current object
            storage.

        """
        ...

    @remote_api_endpoint("content/add")
    def add(
        self, content: bytes, obj_id: HashDict, check_presence: bool = True
    ) -> None:
        """Add a new object to the object storage.

        Args:
            content: object's raw content to add in storage.
            obj_id: dict of checksums.
            check_presence (bool): indicate if the presence of the
                content should be verified before adding the file.

        Returns:
            the id (bytes) of the object into the storage.

        """
        ...

    @remote_api_endpoint("content/add/batch")
    def add_batch(
        self,
        contents: Iterable[Tuple[HashDict, bytes]],
        check_presence: bool = True,
    ) -> Dict:
        """Add a batch of new objects to the object storage.

        Args:
            contents: list of pairs of composite object ids and object contents

        Returns:
            the summary of objects added to the storage (count of object,
            count of bytes object)

        """
        ...

    def restore(self, content: bytes, obj_id: HashDict) -> None:
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
    def get(self, obj_id: HashDict) -> bytes:
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
    def get_batch(self, obj_ids: Iterable[HashDict]) -> Iterator[Optional[bytes]]:
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

    @remote_api_endpoint("content/download_url")
    def download_url(
        self,
        obj_id: HashDict,
        content_disposition: Optional[str] = None,
        expiry: Optional[timedelta] = None,
    ) -> Optional[str]:
        """Get a direct download link for the object if the obstorage backend supports
        such feature.

        Some objstorage backends, typically cloud based ones like azure or s3, can provide
        a direct download link for a stored object.

        Args:
            obj_id: object identifier
            content_disposition: set Content-Disposition header for the generated URL
                response if the objstorage backend supports it
            expiry: the duration after which the URL expires if the objstorage backend
                supports it, if not provided the URL expires 24 hours after its creation

        Returns:
            Direct download URL for the object or :const:`None` if the objstorage backend does
                not support such feature.
        """

        ...

    @remote_api_endpoint("content/check")
    def check(self, obj_id: HashDict) -> None:
        """Perform an integrity check for a given object.

        Verify that the file object is in place and that the content matches
        the object id.

        Args:
            obj_id: object identifier.

        Raises:
            ObjNotFoundError: if the requested object is missing.
            ObjCorruptedError: if the requested object is corrupted.

        """
        ...

    @remote_api_endpoint("content/delete")
    def delete(self, obj_id: HashDict):
        """Delete an object.

        Args:
            obj_id: object identifier.

        Raises:
            ObjNotFoundError: if the requested object is missing.

        """
        ...
