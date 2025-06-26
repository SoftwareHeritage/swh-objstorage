# Copyright (C) 2016-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
from collections import OrderedDict
from datetime import timedelta
from io import BytesIO
import logging
from typing import Optional
from urllib.parse import urlencode

from libcloud.storage import providers
import libcloud.storage.drivers.s3
from libcloud.storage.types import ObjectDoesNotExistError, Provider

from swh.objstorage.constants import LiteralPrimaryHash
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.interface import ObjId
from swh.objstorage.objstorage import (
    CompressionFormat,
    ObjStorage,
    objid_to_default_hex,
    timed,
)

logger = logging.getLogger(__name__)


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

    primary_hash: LiteralPrimaryHash = "sha1"
    name: str = "cloud"

    def __init__(
        self,
        container_name: str,
        compression: CompressionFormat | None = None,
        path_prefix: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.driver = self._get_driver(**kwargs)
        self.container_name = container_name
        self.container = self.driver.get_container(container_name=container_name)
        if compression is None:
            logger.warning("Compression is undefined: defaulting to gzip")
            compression = "gzip"
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
        return self.decompress(obj, objid_to_default_hex(obj_id, self.primary_hash))

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
        primary_hash = obj_id[self.primary_hash]

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
