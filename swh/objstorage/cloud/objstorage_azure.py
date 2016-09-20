# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import gzip

from swh.core import hashutil
from swh.objstorage.objstorage import ObjStorage, compute_hash
from swh.objstorage.exc import ObjNotFoundError, Error

from azure.storage.blob import BlockBlobService
from azure.common import AzureMissingResourceHttpError


class AzureCloudObjStorage(ObjStorage):
    """ObjStorage with azure abilities

    """
    def __init__(self, account_name, api_secret_key, container_name):
        self.block_blob_service = BlockBlobService(
            account_name=account_name,
            account_key=api_secret_key)
        self.container_name = container_name

    def __contains__(self, obj_id):
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        return self.block_blob_service.exists(
            container_name=self.container_name,
            blob_name=hex_obj_id)

    def __iter__(self):
        """ Iterate over the objects present in the storage

        """
        for obj in self.block_blob_service.list_blobs(self.container_name):
            yield obj.name

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

        hex_obj_id = hashutil.hash_to_hex(obj_id)

        # Send the gzipped content
        self.block_blob_service.create_blob_from_bytes(
            container_name=self.container_name,
            blob_name=hex_obj_id,
            blob=gzip.compress(content))

        return obj_id

    def restore(self, content, obj_id=None):
        return self.add(content, obj_id, check_presence=False)

    def get(self, obj_id):
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        try:
            blob = self.block_blob_service.get_blob_to_bytes(
                container_name=self.container_name,
                blob_name=hex_obj_id)
        except AzureMissingResourceHttpError:
            raise ObjNotFoundError('Content %s not found!' % hex_obj_id)

        return gzip.decompress(blob.content)

    def check(self, obj_id):
        # Check the content integrity
        obj_content = self.get(obj_id)
        content_obj_id = compute_hash(obj_content)
        if content_obj_id != obj_id:
            raise Error(obj_id)
