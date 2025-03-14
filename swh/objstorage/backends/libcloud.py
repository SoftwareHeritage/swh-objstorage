# Copyright (C) 2016-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
from collections import OrderedDict
from datetime import timedelta
from io import BytesIO
from typing import Iterator, Optional
from urllib.parse import urlencode

from libcloud.storage import providers
import libcloud.storage.drivers.s3
from libcloud.storage.types import ObjectDoesNotExistError, Provider

from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.interface import ObjId
from swh.objstorage.objstorage import (
    CompressionFormat,
    ObjStorage,
    objid_to_default_hex,
    timed,
)


def patch_libcloud_s3_urlencode():
    """Patches libcloud's S3 backend to properly sign queries.

    Recent versions of libcloud are not affected (they use signature V4),
    but 1.5.0 (the one in Debian 9) is."""

    def s3_urlencode(params):
        """Like urllib.parse.urlencode, but sorts the parameters first.
        This is required to properly compute the request signature, see
        https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#ConstructingTheCanonicalizedResourceElement
        """  # noqa
        return urlencode(OrderedDict(sorted(params.items())))

    libcloud.storage.drivers.s3.urlencode = s3_urlencode


patch_libcloud_s3_urlencode()


class CloudObjStorage(ObjStorage, metaclass=abc.ABCMeta):
    """Abstract ObjStorage that connect to a cloud using Libcloud

    Implementations of this class must redefine the _get_provider
    method to make it return a driver provider (i.e. object that
    supports `get_driver` method) which return a LibCloud driver (see
    https://libcloud.readthedocs.io/en/latest/storage/api.html).

    Args:
      container_name: Name of the base container
      path_prefix: prefix to prepend to object paths in the container,
                   separated with a slash
      compression: compression algorithm to use for objects
      kwargs: extra arguments are passed through to the LibCloud driver
    """

    PRIMARY_HASH = "sha1"
    name: str = "cloud"

    def __init__(
        self,
        container_name: str,
        compression: CompressionFormat = "gzip",
        path_prefix: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.driver = self._get_driver(**kwargs)
        self.container_name = container_name
        self.container = self.driver.get_container(container_name=container_name)
        self.compression = compression
        self.path_prefix = None
        if path_prefix:
            self.path_prefix = path_prefix.rstrip("/") + "/"

    def _get_driver(self, **kwargs):
        """Initialize a driver to communicate with the cloud

        Kwargs: arguments passed to the StorageDriver class, typically
          key: key to connect to the API.
          secret: secret key for authentication.
          secure: (bool) support HTTPS
          host: (str)
          port: (int)
          api_version: (str)
          region: (str)

        Returns:
            a Libcloud driver to a cloud storage.

        """
        # Get the driver class from its description.
        cls = providers.get_driver(self._get_provider())
        # Initialize the driver.
        return cls(**kwargs)

    @abc.abstractmethod
    def _get_provider(self):
        """Get a libcloud driver provider

        This method must be overridden by subclasses to specify which
        of the native libcloud driver the current storage should
        connect to.  Alternatively, provider for a custom driver may
        be returned, in which case the provider will have to support
        `get_driver` method.

        """
        raise NotImplementedError(
            "%s must implement `get_provider` method" % type(self)
        )

    def check_config(self, *, check_write):
        """Check the configuration for this object storage"""
        # FIXME: hopefully this blew up during instantiation
        return True

    @timed
    def __contains__(self, obj_id: ObjId) -> bool:
        try:
            self._get_object(obj_id)
        except ObjNotFoundError:
            return False
        else:
            return True

    def __iter__(self) -> Iterator[ObjId]:
        """Iterate over the objects present in the storage

        Warning: Iteration over the contents of a cloud-based object storage
        may have bad efficiency: due to the very high amount of objects in it
        and the fact that it is remote, get all the contents of the current
        object storage may result in a lot of network requests.

        You almost certainly don't want to use this method in production.
        """
        for obj in self.driver.iterate_container_objects(self.container):
            name = obj.name

            if self.path_prefix and not name.startswith(self.path_prefix):
                continue

            if self.path_prefix:
                name = name[len(self.path_prefix) :]

            if self.PRIMARY_HASH == "sha1":
                yield {"sha1": bytes.fromhex(name)}
            elif self.PRIMARY_HASH == "sha256":
                yield {"sha256": bytes.fromhex(name)}
            else:
                raise ValueError(f"Unknown primary hash {self.PRIMARY_HASH}")

    def __len__(self):
        """Compute the number of objects in the current object storage.

        Warning: this currently uses `__iter__`, its warning about bad
        performance applies.

        Returns:
            number of objects contained in the storage.

        """
        return sum(1 for i in self)

    @timed
    def add(self, content: bytes, obj_id: ObjId, check_presence: bool = True) -> None:
        if check_presence and obj_id in self:
            return

        self._put_object(content, obj_id)

    def restore(self, content: bytes, obj_id: ObjId) -> None:
        return self.add(content, obj_id, check_presence=False)

    @timed
    def get(self, obj_id: ObjId) -> bytes:
        obj = b"".join(self._get_object(obj_id).as_stream())
        return self.decompress(obj, objid_to_default_hex(obj_id))

    def download_url(
        self,
        obj_id: ObjId,
        content_disposition: Optional[str] = None,
        expiry: Optional[timedelta] = None,
    ) -> Optional[str]:
        return self._get_object(obj_id).get_cdn_url()

    def delete(self, obj_id: ObjId):
        super().delete(obj_id)  # Check delete permission
        obj = self._get_object(obj_id)
        return self.driver.delete_object(obj)

    def _object_path(self, obj_id: ObjId) -> str:
        """Get the full path to an object"""
        primary_hash = obj_id[self.PRIMARY_HASH]

        hex_primary_hash = primary_hash.hex()
        if self.path_prefix:
            return self.path_prefix + hex_primary_hash
        else:
            return hex_primary_hash

    def _get_object(self, obj_id: ObjId):
        """Get a Libcloud wrapper for an object pointer.

        This wrapper does not retrieve the content of the object
        directly.

        """
        object_path = self._object_path(obj_id)

        try:
            return self.driver.get_object(self.container_name, object_path)
        except ObjectDoesNotExistError:
            raise ObjNotFoundError(obj_id)

    def _put_object(self, content, obj_id):
        """Create an object in the cloud storage.

        Created object will contain the content and be referenced by
        the given id.

        """
        object_path = self._object_path(obj_id)
        self.driver.upload_object_via_stream(
            BytesIO(self.compress(content)),
            self.container,
            object_path,
        )


class AwsCloudObjStorage(CloudObjStorage):
    """Amazon's S3 Cloud-based object storage"""

    name: str = "s3"

    def _get_provider(self):
        return Provider.S3

    def download_url(
        self,
        obj_id: ObjId,
        content_disposition: Optional[str] = None,
        expiry: Optional[timedelta] = None,
    ) -> Optional[str]:
        return self.driver.get_object_cdn_url(
            self._get_object(obj_id),
            ex_expiry=(expiry.total_seconds() / 3600) if expiry is not None else 24,
        )


class OpenStackCloudObjStorage(CloudObjStorage):
    """OpenStack Swift Cloud based object storage"""

    name: str = "swift"

    def _get_provider(self):
        return Provider.OPENSTACK_SWIFT
