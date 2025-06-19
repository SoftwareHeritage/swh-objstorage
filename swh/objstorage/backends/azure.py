# Copyright (C) 2016-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio
import contextlib
from datetime import datetime, timedelta, timezone
from itertools import product
import string
from typing import Iterable, Iterator, Mapping, Optional, Union
from urllib.parse import parse_qs, urlparse
import warnings

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import (
    BlobSasPermissions,
    ContainerClient,
    ContainerSasPermissions,
    generate_blob_sas,
    generate_container_sas,
)
from azure.storage.blob.aio import ContainerClient as AsyncContainerClient

from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.interface import ObjId
from swh.objstorage.objstorage import CompressionFormat, ObjStorage, timed
from swh.objstorage.utils import call_async


def get_container_url(
    account_name: str,
    account_key: str,
    container_name: str,
    access_policy: str = "read_only",
    expiry: timedelta = timedelta(days=365),
    container_url_template: str = (
        "https://{account_name}.blob.core.windows.net/{container_name}?{signature}"
    ),
    **kwargs,
) -> str:
    """Get the full url, for the given container on the given account, with a
    Shared Access Signature granting the specified access policy.

    Args:
      account_name: name of the storage account for which to generate the URL
      account_key: shared account key of the storage account used to generate the SAS
      container_name: name of the container for which to grant access in the storage
        account
      access_policy: one of ``read_only``, ``append_only``, ``full``
      expiry: the interval in the future with which the signature will expire

    Returns:
      the full URL of the container, with the shared access signature.
    """

    access_policies = {
        "read_only": ContainerSasPermissions(
            read=True, list=True, delete=False, write=False
        ),
        "append_only": ContainerSasPermissions(
            read=True, list=True, delete=False, write=True
        ),
        "full": ContainerSasPermissions(read=True, list=True, delete=True, write=True),
    }

    current_time = datetime.utcnow()

    signature = generate_container_sas(
        account_name,
        container_name,
        account_key=account_key,
        permission=access_policies[access_policy],
        start=current_time + timedelta(minutes=-1),
        expiry=current_time + expiry,
    )

    return container_url_template.format(
        account_name=account_name,
        container_name=container_name,
        signature=signature,
    )


class AzureCloudObjStorage(ObjStorage):
    """ObjStorage backend for Azure blob storage accounts.

    Args:
      container_url: the URL of the container in which the objects are stored.
      account_name: (deprecated) the name of the storage account under which objects are
        stored
      api_secret_key: (deprecated) the shared account key
      container_name: (deprecated) the name of the container under which objects are
        stored
      compression: the compression algorithm used to compress objects in storage
      use_secondary_endpoint_for_downloads: if True, use the secondary endpoint
        url to generate download URLs. To configure the secondary endpoint, use
        the BlobSecondaryEndpoint entry of the connection string.

    Notes:
      The container url should contain the credentials via a "Shared Access
      Signature". The :func:`get_container_url` helper can be used to generate
      such a URL from the account's access keys. The ``account_name``,
      ``api_secret_key`` and ``container_name`` arguments are deprecated.

    """

    PRIMARY_HASH = "sha1"
    name: str = "azure"

    def __init__(
        self,
        *,
        container_url: Optional[str] = None,
        account_name: Optional[str] = None,
        api_secret_key: Optional[str] = None,
        container_name: Optional[str] = None,
        connection_string: Optional[str] = None,
        compression: CompressionFormat = "gzip",
        use_secondary_endpoint_for_downloads=False,
        **kwargs,
    ):
        if container_url is None and connection_string is None:
            if account_name is None or api_secret_key is None or container_name is None:
                raise ValueError(
                    "AzureCloudObjStorage must have a container_url, a connection_string,"
                    "or all three account_name, api_secret_key and container_name"
                )
            else:
                warnings.warn(
                    "The Azure objstorage account secret key parameters are "
                    "deprecated, please use container URLs instead.",
                    DeprecationWarning,
                )
                container_url = get_container_url(
                    account_name=account_name,
                    account_key=api_secret_key,
                    container_name=container_name,
                    access_policy="full",
                )
        elif connection_string:
            if container_name is None:
                raise ValueError(
                    "container_name is required when using connection_string."
                )
            self.container_name = container_name

        super().__init__(**kwargs)
        self.container_url = container_url
        self.connection_string = connection_string
        self.compression = compression
        self.use_secondary = use_secondary_endpoint_for_downloads

    def get_container_client(self, hex_obj_id):
        """Get the container client for the container that contains the object with
        internal id hex_obj_id

        This is used to allow the PrefixedAzureCloudObjStorage to dispatch the
        client according to the prefix of the object id.

        """
        if self.connection_string:
            return ContainerClient.from_connection_string(
                self.connection_string, self.container_name
            )
        else:
            return ContainerClient.from_container_url(self.container_url)

    @contextlib.asynccontextmanager
    async def get_async_container_clients(self):
        """Returns a collection of container clients, to be passed to
        ``get_async_blob_client``.

        Each container may not be used in more than one asyncio loop."""
        if self.connection_string:
            client = AsyncContainerClient.from_connection_string(
                self.connection_string, self.container_name
            )
        else:
            client = AsyncContainerClient.from_container_url(self.container_url)
        async with client:
            yield {"": client}

    def get_blob_client(self, hex_obj_id):
        """Get the azure blob client for the given hex obj id"""
        container_client = self.get_container_client(hex_obj_id)

        return container_client.get_blob_client(blob=hex_obj_id)

    def get_async_blob_client(self, hex_obj_id, container_clients):
        """Get the azure blob client for the given hex obj id and a collection
        yielded by ``get_async_container_clients``."""

        return container_clients[""].get_blob_client(blob=hex_obj_id)

    def get_all_container_clients(self):
        """Get all active block_blob_services"""
        yield self.get_container_client("")

    def _internal_id(self, obj_id: ObjId) -> str:
        """Internal id is the hex version in objstorage."""
        primary_hash = obj_id[self.PRIMARY_HASH]
        return primary_hash.hex()

    def check_config(self, *, check_write):
        """Check the configuration for this object storage"""
        now = datetime.now(tz=timezone.utc).isoformat()
        for container_client in self.get_all_container_clients():
            parsed = urlparse(container_client.url)
            # The query string for the container_client url contains a bunch of
            # fields that are associated with the permissions given by the
            # shared access signature. We treat all fields as optional, this is
            # not a 100% bullet-proof set of checks, for instance if a
            # connection_string is used
            qs = parse_qs(parsed.query)

            # st is the "signed start" (the signature is only valid after that
            # date) as an ISO-8601 encoded datetime
            if "st" in qs and qs["st"][0] > now:
                raise ValueError(
                    "Shared access signature is not valid yet: %s" % qs["st"][0]
                )

            # se is the "signed expiry" (the signature is invalid after that
            # date) as an ISO-8601 encoded datetime
            if "se" in qs and qs["se"][0] < now:
                raise ValueError(
                    "Shared access signature has expired: %s" % qs["se"][0]
                )

            # sr is the "signed resource". "c" means container.
            if "sr" in qs and "c" not in qs["sr"][0]:
                raise ValueError(
                    "Shared access signature is not for a container service"
                )

            # sp is the "signed permissions"
            if (
                "sp" in qs
                and check_write
                # "c" allows to create new objects
                and "c" not in qs["sp"][0]
                # "w" allows to write to objects. Either permission is valid to
                # write to an objstorage
                and "w" not in qs["sp"][0]
            ):
                # We have neither "c" or "w" permissions, this is read-only
                return False

        return True

    @timed
    def __contains__(self, obj_id: ObjId) -> bool:
        """Does the storage contains the obj_id."""
        hex_obj_id = self._internal_id(obj_id)
        client = self.get_blob_client(hex_obj_id)
        try:
            client.get_blob_properties()
        except ResourceNotFoundError:
            return False
        else:
            return True

    def __iter__(self) -> Iterator[ObjId]:
        """Iterate over the objects present in the storage."""
        for client in self.get_all_container_clients():
            for obj in client.list_blobs():
                if self.PRIMARY_HASH == "sha1":
                    yield {"sha1": bytes.fromhex(obj.name)}
                elif self.PRIMARY_HASH == "sha256":
                    yield {"sha256": bytes.fromhex(obj.name)}
                else:
                    raise ValueError(f"Unknown primary hash {self.PRIMARY_HASH}")

    def __len__(self):
        """Compute the number of objects in the current object storage.

        Returns:
            number of objects contained in the storage.

        """
        return sum(1 for i in self)

    @timed
    def add(self, content: bytes, obj_id: ObjId, check_presence: bool = True) -> None:
        """Add an obj in storage if it's not there already."""
        if check_presence and obj_id in self:
            return

        hex_obj_id = self._internal_id(obj_id)

        # Send the compressed content
        data = self.compress(content)

        client = self.get_blob_client(hex_obj_id)
        try:
            client.upload_blob(data=data, length=len(data))
        except ResourceExistsError:
            # There's a race condition between check_presence and upload_blob,
            # that we can't get rid of as the azure api doesn't allow atomic
            # replaces or renaming a blob. As the restore operation explicitly
            # removes the blob, it should be safe to just ignore the error.
            pass

    def restore(self, content: bytes, obj_id: ObjId) -> None:
        """Restore a content."""
        if obj_id in self:
            self.delete(obj_id)

        return self.add(content, obj_id, check_presence=False)

    @timed
    def get(self, obj_id: ObjId) -> bytes:
        """retrieve blob's content if found."""
        return call_async(self._get_async, obj_id)

    async def _get_async(self, obj_id, container_clients=None):
        """Coroutine implementing ``get(obj_id)`` using azure-storage-blob's
        asynchronous implementation.
        While ``get(obj_id)`` does not need asynchronicity, this is useful to
        ``get_batch(obj_ids)``, as it can run multiple ``_get_async`` tasks
        concurrently."""
        if container_clients is None:
            # If the container_clients argument is not passed, create a new
            # collection of container_clients and restart the function with it.
            async with self.get_async_container_clients() as container_clients:
                return await self._get_async(obj_id, container_clients)

        hex_obj_id = self._internal_id(obj_id)
        client = self.get_async_blob_client(hex_obj_id, container_clients)

        try:
            download = await client.download_blob()
        except ResourceNotFoundError:
            raise ObjNotFoundError(obj_id) from None
        else:
            data = await download.content_as_bytes()

        return self.decompress(data, hex_obj_id)

    async def _get_async_or_none(self, obj_id, container_clients):
        """Like ``get_async(obj_id)``, but returns None instead of raising
        ResourceNotFoundError. Used by ``get_batch`` so other blobs can be returned
        even if one is missing."""
        try:
            return await self._get_async(obj_id, container_clients)
        except ObjNotFoundError:
            return None

    async def _get_batch_async(self, obj_ids):
        async with self.get_async_container_clients() as container_clients:
            return await asyncio.gather(
                *[
                    self._get_async_or_none(obj_id, container_clients)
                    for obj_id in obj_ids
                ]
            )

    def get_batch(self, obj_ids: Iterable[ObjId]) -> Iterator[Optional[bytes]]:
        """Retrieve objects' raw content in bulk from storage, concurrently."""
        return call_async(self._get_batch_async, obj_ids)

    def delete(self, obj_id: ObjId):
        """Delete an object."""
        super().delete(obj_id)  # Check delete permission
        hex_obj_id = self._internal_id(obj_id)
        client = self.get_blob_client(hex_obj_id)
        try:
            client.delete_blob()
        except ResourceNotFoundError:
            raise ObjNotFoundError(obj_id) from None

        return True

    def download_url(
        self,
        obj_id: ObjId,
        content_disposition: Optional[str] = None,
        expiry: Optional[timedelta] = None,
    ) -> Optional[str]:
        hex_obj_id = self._internal_id(obj_id)
        client = self.get_blob_client(hex_obj_id)
        try:
            client.get_blob_properties()
        except ResourceNotFoundError:
            raise ObjNotFoundError(obj_id)
        else:
            signature = generate_blob_sas(
                client.account_name,
                client.container_name,
                hex_obj_id,
                account_key=client.credential.account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.now() + (expiry or timedelta(hours=24)),
                content_disposition=content_disposition,
            )
            if self.use_secondary:
                return f"{client.secondary_endpoint}?{signature}"
            else:
                return f"{client.primary_endpoint}?{signature}"


class PrefixedAzureCloudObjStorage(AzureCloudObjStorage):
    """ObjStorage with azure capabilities, striped by prefix.

    accounts is a dict containing entries of the form:
        <prefix>: <container_url_for_prefix>
    """

    def __init__(
        self,
        accounts: Mapping[str, Union[str, Mapping[str, str]]],
        name: str = "azure-prefixed",
        compression: CompressionFormat = "gzip",
        **kwargs,
    ):
        # shortcut AzureCloudObjStorage __init__
        ObjStorage.__init__(
            self,
            name=name,
            **kwargs,
        )

        self.compression = compression

        # Definition sanity check
        prefix_lengths = set(len(prefix) for prefix in accounts)
        if not len(prefix_lengths) == 1:
            raise ValueError(
                "Inconsistent prefixes, found lengths %s"
                % ", ".join(str(lst) for lst in sorted(prefix_lengths))
            )

        self.prefix_len = prefix_lengths.pop()

        expected_prefixes = set(
            "".join(letters)
            for letters in product(
                set(string.hexdigits.lower()), repeat=self.prefix_len
            )
        )
        missing_prefixes = expected_prefixes - set(accounts)
        if missing_prefixes:
            raise ValueError(
                "Missing prefixes %s" % ", ".join(sorted(missing_prefixes))
            )

        do_warning = False

        self.container_urls = {}
        for prefix, container_url in accounts.items():
            if isinstance(container_url, dict):
                do_warning = True
                container_url = get_container_url(
                    account_name=container_url["account_name"],
                    account_key=container_url["api_secret_key"],
                    container_name=container_url["container_name"],
                    access_policy="full",
                )
            self.container_urls[prefix] = container_url

        if do_warning:
            warnings.warn(
                "The Azure objstorage account secret key parameters are "
                "deprecated, please use container URLs instead.",
                DeprecationWarning,
            )

    def get_container_client(self, hex_obj_id):
        """Get the block_blob_service and container that contains the object with
        internal id hex_obj_id
        """
        prefix = hex_obj_id[: self.prefix_len]
        return ContainerClient.from_container_url(self.container_urls[prefix])

    @contextlib.asynccontextmanager
    async def get_async_container_clients(self):
        # This is equivalent to:
        # client1 = AsyncContainerClient.from_container_url(url1)
        # ...
        # client16 = AsyncContainerClient.from_container_url(url16)
        # async with client1, ..., client16:
        #     yield {prefix1: client1, ..., prefix16: client16}
        clients = {
            prefix: AsyncContainerClient.from_container_url(url)
            for (prefix, url) in self.container_urls.items()
        }
        async with contextlib.AsyncExitStack() as stack:
            for client in clients.values():
                await stack.enter_async_context(client)
            yield clients

    def get_async_blob_client(self, hex_obj_id, container_clients):
        """Get the azure blob client for the given hex obj id and a collection
        yielded by ``get_async_container_clients``."""

        prefix = hex_obj_id[: self.prefix_len]
        return container_clients[prefix].get_blob_client(blob=hex_obj_id)

    def get_all_container_clients(self):
        """Get all active container clients"""
        # iterate on items() to sort blob services;
        yield from (
            self.get_container_client(prefix) for prefix in sorted(self.container_urls)
        )
