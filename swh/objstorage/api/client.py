# Copyright (C) 2015-2016  The Software Heritage developers
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
        """ Indicates if the given object is present in the storage

        See base class [ObjStorage].
        """
        return self.post('content/contains', {'obj_id': obj_id})

    def add(self, content, obj_id=None, check_presence=True):
        """ Add a new object to the object storage.

        See base class [ObjStorage].
        """
        return self.post('content/add', {'content': content, 'obj_id': obj_id,
                                         'check_presence': check_presence})

    def get(self, obj_id):
        """ Retrieve the content of a given object.

        See base class [ObjStorage].
        """
        return self.post('content/get', {'obj_id': obj_id})

    def get_batch(self, obj_ids):
        """ Retrieve content in bulk.

        See base class [ObjStorage].
        """
        return self.post('content/get/batch', {'obj_ids': obj_ids})

    def check(self, obj_id):
        """ Perform an integrity check for a given object.

        See base class [ObjStorage].
        """
        self.post('content/check', {'obj_id': obj_id})

    def get_random(self, batch_size):
        """ Get random ids of existing contents

        See base class [ObjStorage].
        """
        return self.post('content/get/random', {'batch_size': batch_size})
