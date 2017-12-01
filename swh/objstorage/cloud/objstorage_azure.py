# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import gzip

from swh.objstorage.objstorage import ObjStorage, compute_hash
from swh.objstorage.exc import ObjNotFoundError, Error
from swh.model import hashutil

from azure.storage.blob import BlockBlobService
from azure.common import AzureMissingResourceHttpError


class AzureCloudObjStorage(ObjStorage):
    """ObjStorage with azure abilities.

    """
    def __init__(self, account_name, api_secret_key, container_name, **kwargs):
        super().__init__(**kwargs)
        self.block_blob_service = BlockBlobService(
            account_name=account_name,
            account_key=api_secret_key)
        self.container_name = container_name

    def _internal_id(self, obj_id):
        """Internal id is the hex version in objstorage.

        """
        return hashutil.hash_to_hex(obj_id)

    def check_config(self, *, check_write):
        """Check the configuration for this object storage"""
        props = self.block_blob_service.get_container_properties(
            self.container_name
        )

        # FIXME: check_write is ignored here
        return bool(props)

    def __contains__(self, obj_id):
        """Does the storage contains the obj_id.

        """
        hex_obj_id = self._internal_id(obj_id)
        return self.block_blob_service.exists(
            container_name=self.container_name,
            blob_name=hex_obj_id)

    def __iter__(self):
        """Iterate over the objects present in the storage.

        """
        for obj in self.block_blob_service.list_blobs(self.container_name):
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
        self.block_blob_service.create_blob_from_bytes(
            container_name=self.container_name,
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
        try:
            blob = self.block_blob_service.get_blob_to_bytes(
                container_name=self.container_name,
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
        try:
            self.block_blob_service.delete_blob(
                container_name=self.container_name,
                blob_name=hex_obj_id)
        except AzureMissingResourceHttpError:
            raise ObjNotFoundError('Content {} not found!'.format(hex_obj_id))

        return True
