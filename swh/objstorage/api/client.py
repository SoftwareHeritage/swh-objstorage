# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Iterator, Optional

import msgpack

from swh.core.api import RPCClient
from swh.objstorage.constants import DEFAULT_LIMIT
from swh.objstorage.exc import Error, ObjNotFoundError, ObjStorageAPIError
from swh.objstorage.interface import CompositeObjId, ObjId, ObjStorageInterface
from swh.objstorage.objstorage import objid_to_default_hex


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

    def restore(self: ObjStorageInterface, content: bytes, obj_id: ObjId) -> None:
        return self.add(content, obj_id, check_presence=False)

    def __iter__(self) -> Iterator[CompositeObjId]:
        yield from self.list_content()

    def list_content(
        self,
        last_obj_id: Optional[ObjId] = None,
        limit: Optional[int] = DEFAULT_LIMIT,
    ) -> Iterator[CompositeObjId]:
        params: Dict[str, Any] = {}
        if limit:
            params["limit"] = limit
        if last_obj_id:
            params["last_obj_id"] = objid_to_default_hex(last_obj_id)
        response = self.raw_verb(
            "get",
            "content",
            headers={"accept": "application/x-msgpack"},
            params=params,
            stream=True,
        )
        yield from msgpack.Unpacker(response.raw, raw=False)
