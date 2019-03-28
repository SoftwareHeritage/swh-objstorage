# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.core.api import SWHRemoteAPI
from swh.model import hashutil

from ..objstorage import DEFAULT_CHUNK_SIZE, DEFAULT_LIMIT
from ..exc import ObjNotFoundError, ObjStorageAPIError


class RemoteObjStorage:
    """Proxy to a remote object storage.

    This class allows to connect to an object storage server via
    http protocol.

    Attributes:
        url (string): The url of the server to connect. Must end
            with a '/'
        session: The session to send requests.

    """

    def __init__(self, **kwargs):
        self._proxy = SWHRemoteAPI(api_exception=ObjStorageAPIError, **kwargs)

    def check_config(self, *, check_write):
        return self._proxy.post('check_config', {'check_write': check_write})

    def __contains__(self, obj_id):
        return self._proxy.post('content/contains', {'obj_id': obj_id})

    def add(self, content, obj_id=None, check_presence=True):
        return self._proxy.post('content/add', {
            'content': content, 'obj_id': obj_id,
            'check_presence': check_presence})

    def add_batch(self, contents, check_presence=True):
        return self._proxy.post('content/add/batch', {
            'contents': contents,
            'check_presence': check_presence,
        })

    def restore(self, content, obj_id=None, *args, **kwargs):
        return self.add(content, obj_id, check_presence=False)

    def get(self, obj_id):
        ret = self._proxy.post('content/get', {'obj_id': obj_id})
        if ret is None:
            raise ObjNotFoundError(obj_id)
        else:
            return ret

    def get_batch(self, obj_ids):
        return self._proxy.post('content/get/batch', {'obj_ids': obj_ids})

    def check(self, obj_id):
        return self._proxy.post('content/check', {'obj_id': obj_id})

    def delete(self, obj_id):
        # deletion permission are checked server-side
        return self._proxy.post('content/delete', {'obj_id': obj_id})

    # Management methods

    def get_random(self, batch_size):
        return self._proxy.post('content/get/random',
                                {'batch_size': batch_size})

    # Streaming methods

    def add_stream(self, content_iter, obj_id, check_presence=True):
        obj_id = hashutil.hash_to_hex(obj_id)
        return self._proxy.post_stream(
            'content/add_stream/{}'.format(obj_id),
            params={'check_presence': check_presence},
            data=content_iter)

    def get_stream(self, obj_id, chunk_size=DEFAULT_CHUNK_SIZE):
        obj_id = hashutil.hash_to_hex(obj_id)
        return self._proxy.get_stream('content/get_stream/{}'.format(obj_id),
                                      chunk_size=chunk_size)

    def __iter__(self):
        yield from self._proxy.get_stream('content')

    def list_content(self, last_obj_id=None, limit=DEFAULT_LIMIT):
        params = {'limit': limit}
        if last_obj_id:
            params['last_obj_id'] = hashutil.hash_to_hex(last_obj_id)
        yield from self._proxy.get_stream('content', params=params)
