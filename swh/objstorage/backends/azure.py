# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import gzip
import string
from itertools import dropwhile, islice, product

from azure.storage.blob import BlockBlobService
from azure.common import AzureMissingResourceHttpError
import requests

from swh.objstorage.objstorage import ObjStorage, compute_hash, DEFAULT_LIMIT
from swh.objstorage.exc import ObjNotFoundError, Error
from swh.model import hashutil


class AzureCloudObjStorage(ObjStorage):
    """ObjStorage with azure abilities.

    """
    def __init__(self, account_name, api_secret_key, container_name, **kwargs):
        super().__init__(**kwargs)
        self.block_blob_service = BlockBlobService(
            account_name=account_name,
            account_key=api_secret_key,
            request_session=requests.Session(),
        )
        self.container_name = container_name

    def get_blob_service(self, hex_obj_id):
        """Get the block_blob_service and container that contains the object with
        internal id hex_obj_id
        """
        return self.block_blob_service, self.container_name

    def get_all_blob_services(self):
        """Get all active block_blob_services"""
        yield self.block_blob_service, self.container_name

    def _internal_id(self, obj_id):
        """Internal id is the hex version in objstorage.

        """
        return hashutil.hash_to_hex(obj_id)

    def check_config(self, *, check_write):
        """Check the configuration for this object storage"""
        for service, container in self.get_all_blob_services():
            props = service.get_container_properties(container)

            # FIXME: check_write is ignored here
            if not props:
                return False

        return True

    def __contains__(self, obj_id):
        """Does the storage contains the obj_id.

        """
        hex_obj_id = self._internal_id(obj_id)
        service, container = self.get_blob_service(hex_obj_id)
        return service.exists(
            container_name=container,
            blob_name=hex_obj_id)

    def __iter__(self):
        """Iterate over the objects present in the storage.

        """
        for service, container in self.get_all_blob_services():
            for obj in service.list_blobs(container):
                yield hashutil.hash_to_bytes(obj.name)

    def __len__(self):
        """Compute the number of objects in the current object storage.

        Returns:
            number of objects contained in the storage.

        """
        return sum(1 for i in self)

    def add(self, content, obj_id=None, check_presence=True):
        """Add an obj in storage if it's not there already.

        """
        if obj_id is None:
            # Checksum is missing, compute it on the fly.
            obj_id = compute_hash(content)

        if check_presence and obj_id in self:
            return obj_id

        hex_obj_id = self._internal_id(obj_id)

        # Send the gzipped content
        service, container = self.get_blob_service(hex_obj_id)
        service.create_blob_from_bytes(
            container_name=container,
            blob_name=hex_obj_id,
            blob=gzip.compress(content))

        return obj_id

    def restore(self, content, obj_id=None):
        """Restore a content.

        """
        return self.add(content, obj_id, check_presence=False)

    def get(self, obj_id):
        """Retrieve blob's content if found.

        """
        hex_obj_id = self._internal_id(obj_id)
        service, container = self.get_blob_service(hex_obj_id)
        try:
            blob = service.get_blob_to_bytes(
                container_name=container,
                blob_name=hex_obj_id)
        except AzureMissingResourceHttpError:
            raise ObjNotFoundError(obj_id)

        return gzip.decompress(blob.content)

    def check(self, obj_id):
        """Check the content integrity.

        """
        obj_content = self.get(obj_id)
        content_obj_id = compute_hash(obj_content)
        if content_obj_id != obj_id:
            raise Error(obj_id)

    def delete(self, obj_id):
        """Delete an object."""
        super().delete(obj_id)  # Check delete permission
        hex_obj_id = self._internal_id(obj_id)
        service, container = self.get_blob_service(hex_obj_id)
        try:
            service.delete_blob(
                container_name=container,
                blob_name=hex_obj_id)
        except AzureMissingResourceHttpError:
            raise ObjNotFoundError('Content {} not found!'.format(hex_obj_id))

        return True

    def list_content(self, last_obj_id=None, limit=DEFAULT_LIMIT):
        all_blob_services = self.get_all_blob_services()
        if last_obj_id:
            last_obj_id = self._internal_id(last_obj_id)
            last_service, _ = self.get_blob_service(last_obj_id)
            all_blob_services = dropwhile(
                lambda srv: srv[0] != last_service, all_blob_services)
        else:
            last_service = None

        def iterate_blobs():
            for service, container in all_blob_services:
                marker = last_obj_id if service == last_service else None
                for obj in service.list_blobs(
                        container, marker=marker, maxresults=limit):
                    yield hashutil.hash_to_bytes(obj.name)
        return islice(iterate_blobs(), limit)


class PrefixedAzureCloudObjStorage(AzureCloudObjStorage):
    """ObjStorage with azure capabilities, striped by prefix.

    accounts is a dict containing entries of the form:
        <prefix>:
          account_name: <account_name>
          api_secret_key: <api_secret_key>
          container_name: <container_name>
    """
    def __init__(self, accounts, **kwargs):
        # shortcut AzureCloudObjStorage __init__
        ObjStorage.__init__(self, **kwargs)

        # Definition sanity check
        prefix_lengths = set(len(prefix) for prefix in accounts)
        if not len(prefix_lengths) == 1:
            raise ValueError("Inconsistent prefixes, found lengths %s"
                             % ', '.join(
                                 str(l) for l in sorted(prefix_lengths)
                             ))

        self.prefix_len = prefix_lengths.pop()

        expected_prefixes = set(
            ''.join(letters)
            for letters in product(
                    set(string.hexdigits.lower()), repeat=self.prefix_len
            )
        )
        missing_prefixes = expected_prefixes - set(accounts)
        if missing_prefixes:
            raise ValueError("Missing prefixes %s"
                             % ', '.join(sorted(missing_prefixes)))

        self.prefixes = {}
        request_session = requests.Session()
        for prefix, account in accounts.items():
            self.prefixes[prefix] = (
                BlockBlobService(
                    account_name=account['account_name'],
                    account_key=account['api_secret_key'],
                    request_session=request_session,
                ),
                account['container_name'],
            )

    def get_blob_service(self, hex_obj_id):
        """Get the block_blob_service and container that contains the object with
        internal id hex_obj_id
        """
        return self.prefixes[hex_obj_id[:self.prefix_len]]

    def get_all_blob_services(self):
        """Get all active block_blob_services"""
        # iterate on items() to sort blob services;
        # needed to be able to paginate in the list_content() method
        yield from (v for _, v in sorted(self.prefixes.items()))
