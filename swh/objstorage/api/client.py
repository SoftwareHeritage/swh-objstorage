# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.core.api import RPCClient
from swh.core.utils import iter_chunks
from swh.model import hashutil
from swh.objstorage.exc import Error, ObjNotFoundError, ObjStorageAPIError
from swh.objstorage.interface import ObjStorageInterface
from swh.objstorage.objstorage import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_LIMIT,
    ID_DIGEST_LENGTH,
)


class RemoteObjStorage(RPCClient):
    """Proxy to a remote object storage.

    This class allows to connect to an object storage server via
    http protocol.

    Attributes:
        url (string): The url of the server to connect. Must end
            with a '/'
        session: The session to send requests.

    """

    api_exception = ObjStorageAPIError
    reraise_exceptions = [ObjNotFoundError, Error]
    backend_class = ObjStorageInterface

    def restore(self, content, obj_id=None):
        return self.add(content, obj_id, check_presence=False)

    def add_stream(self, content_iter, obj_id, check_presence=True):
        raise NotImplementedError

    def get_stream(self, obj_id, chunk_size=DEFAULT_CHUNK_SIZE):
        obj_id = hashutil.hash_to_hex(obj_id)
        return self._get_stream(
            "content/get_stream/{}".format(obj_id), chunk_size=chunk_size
        )

    def __iter__(self):
        yield from self.list_content()

    def list_content(self, last_obj_id=None, limit=DEFAULT_LIMIT):
        params = {"limit": limit}
        if last_obj_id:
            params["last_obj_id"] = hashutil.hash_to_hex(last_obj_id)
        yield from iter_chunks(
            self._get_stream("content", params=params), chunk_size=ID_DIGEST_LENGTH
        )
