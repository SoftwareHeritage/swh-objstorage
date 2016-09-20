# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc

from swh.core import hashutil
from swh.objstorage.objstorage import ObjStorage, compute_hash
from swh.objstorage.exc import ObjNotFoundError, Error

from libcloud.storage import providers
from libcloud.storage.types import Provider, ObjectDoesNotExistError


class CloudObjStorage(ObjStorage, metaclass=abc.ABCMeta):
    """Abstract ObjStorage that connect to a cloud using Libcloud

    Implementations of this class must redefine the _get_provider
    method to make it return a driver provider (i.e. object that
    supports `get_driver` method) which return a LibCloud driver (see
    https://libcloud.readthedocs.io/en/latest/storage/api.html).

    """
    def __init__(self, api_key, api_secret_key, container_name):
        self.driver = self._get_driver(api_key, api_secret_key)
        self.container_name = container_name
        self.container = self.driver.get_container(
            container_name=container_name)

    def _get_driver(self, api_key, api_secret_key):
        """Initialize a driver to communicate with the cloud

        Args:
            api_key: key to connect to the API.
            api_secret_key: secret key for authentification.

        Returns:
            a Libcloud driver to a cloud storage.

        """
        # Get the driver class from its description.
        cls = providers.get_driver(self._get_provider())
        # Initialize the driver.
        return cls(api_key, api_secret_key)

    @abc.abstractmethod
    def _get_provider(self):
        """Get a libcloud driver provider

        This method must be overriden by subclasses to specify which
        of the native libcloud driver the current storage should
        connect to.  Alternatively, provider for a custom driver may
        be returned, in which case the provider will have to support
        `get_driver` method.

        """
        raise NotImplementedError('%s must implement `get_provider` method'
                                  % type(self))

    def __contains__(self, obj_id):
        try:
            self._get_object(obj_id)
        except ObjNotFoundError:
            return False
        else:
            return True

    def __iter__(self):
        """ Iterate over the objects present in the storage

        Warning: Iteration over the contents of a cloud-based object storage
        may have bad efficiency: due to the very high amount of objects in it
        and the fact that it is remote, get all the contents of the current
        object storage may result in a lot of network requests.

        You almost certainly don't want to use this method in production.
        """
        yield from map(lambda obj: obj.name,
                       self.driver.iterate_container_objects(self.container))

    def __len__(self):
        """Compute the number of objects in the current object storage.

        Warning: this currently uses `__iter__`, its warning about bad
        performance applies.

        Returns:
            number of objects contained in the storage.

        """
        return sum(1 for i in self)

    def add(self, content, obj_id=None, check_presence=True):
        if obj_id is None:
            # Checksum is missing, compute it on the fly.
            obj_id = compute_hash(content)

        if check_presence and obj_id in self:
            return obj_id

        self._put_object(content, obj_id)
        return obj_id

    def restore(self, content, obj_id=None):
        return self.add(content, obj_id, check_presence=False)

    def get(self, obj_id):
        return bytes(self._get_object(obj_id).as_stream())

    def check(self, obj_id):
        # Check that the file exists, as _get_object raises ObjNotFoundError
        self._get_object(obj_id)
        # Check the content integrity
        obj_content = self.get(obj_id)
        content_obj_id = compute_hash(obj_content)
        if content_obj_id != obj_id:
            raise Error(obj_id)

    def _get_object(self, obj_id):
        """Get a Libcloud wrapper for an object pointer.

        This wrapper does not retrieve the content of the object
        directly.

        """
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        try:
            return self.driver.get_object(self.container_name, hex_obj_id)
        except ObjectDoesNotExistError as e:
            raise ObjNotFoundError(e.object_name)

    def _put_object(self, content, obj_id):
        """Create an object in the cloud storage.

        Created object will contain the content and be referenced by
        the given id.

        """
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        self.driver.upload_object_via_stream(iter(content), self.container,
                                             hex_obj_id)


class AwsCloudObjStorage(CloudObjStorage):
    """ Amazon's S3 Cloud-based object storage

    """
    def _get_provider(self):
        return Provider.S3


class OpenStackCloudObjStorage(CloudObjStorage):
    """ OpenStack Swift Cloud based object storage

    """
    def _get_provider(self):
        return Provider.OPENSTACK_SWIFT