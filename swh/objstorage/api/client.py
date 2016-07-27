# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import pickle

import requests

from requests.exceptions import ConnectionError
from ..objstorage import ObjStorage
from ..exc import ObjStorageAPIError
from .common import (decode_response,
                     encode_data_client as encode_data)


class RemoteObjStorage(ObjStorage):
    """ Proxy to a remote object storage.

    This class allows to connect to an object storage server via
    http protocol.

    Attributes:
        base_url (string): The url of the server to connect. Must end
            with a '/'
        session: The session to send requests.
    """
    def __init__(self, base_url):
        self.base_url = base_url
        self.session = requests.Session()

    def url(self, endpoint):
        return '%s%s' % (self.base_url, endpoint)

    def post(self, endpoint, data):
        try:
            response = self.session.post(
                self.url(endpoint),
                data=encode_data(data),
                headers={'content-type': 'application/x-msgpack'},
            )
        except ConnectionError as e:
            print(str(e))
            raise ObjStorageAPIError(e)

        # XXX: this breaks language-independence and should be
        # replaced by proper unserialization
        if response.status_code == 400:
            raise pickle.loads(decode_response(response))

        return decode_response(response)

    def __contains__(self, obj_id):
        return self.post('content/contains', {'obj_id': obj_id})

    def add(self, content, obj_id=None, check_presence=True):
        """ Add a new object to the object storage.

        Args:
            content: content of the object to be added to the storage.
            obj_id: checksum of [bytes] using [ID_HASH_ALGO] algorithm. When
                given, obj_id will be trusted to match the bytes. If missing,
                obj_id will be computed on the fly.
            check_presence: indicate if the presence of the content should be
                verified before adding the file.

        Returns:
            the id of the object into the storage.
        """
        return self.post('content/add', {'bytes': content, 'obj_id': obj_id,
                                         'check_presence': check_presence})

    def restore(self, content, obj_id=None):
        """ Restore a content that have been corrupted.

        This function is identical to add_bytes but does not check if
        the object id is already in the file system.

        Args:
            content: content of the object to be added to the storage
            obj_id: checksums of `bytes` as computed by ID_HASH_ALGO. When
                given, obj_id will be trusted to match bytes. If missing,
                obj_id will be computed on the fly.
        """
        return self.add(content, obj_id, check_presence=False)

    def get(self, obj_id):
        """ Retrieve the content of a given object.

        Args:
            obj_id: object id.

        Returns:
            the content of the requested object as bytes.

        Raises:
            ObjNotFoundError: if the requested object is missing.
        """
        return self.post('content/get', {'obj_id': obj_id})

    def get_batch(self, obj_ids):
        """ Retrieve content in bulk.

        Note: This function does have a default implementation in ObjStorage
        that is suitable for most cases.

        Args:
            obj_ids: list of object ids.

        Returns:
            list of resulting contents, or None if the content could not
            be retrieved. Do not raise any exception as a fail for one content
            will not cancel the whole request.
        """
        return self.post('content/get/batch', {'obj_ids': obj_ids})

    def check(self, obj_id):
        """ Perform an integrity check for a given object.

        Verify that the file object is in place and that the gziped content
        matches the object id.

        Args:
            obj_id: object id.

        Raises:
            ObjNotFoundError: if the requested object is missing.
            Error: if the request object is corrupted.
        """
        self.post('content/check', {'obj_id': obj_id})

    def get_random(self, batch_size):
        """ Get random ids of existing contents

        This method is used in order to get random ids to perform
        content integrity verifications on random contents.

        Attributes:
            batch_size (int): Number of ids that will be given

        Yields:
            An iterable of ids of contents that are in the current object
            storage.
        """
        return self.post('content/get/random', {'batch_size': batch_size})
