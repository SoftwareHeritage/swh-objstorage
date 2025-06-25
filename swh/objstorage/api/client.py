# Copyright (C) 2015-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Optional

from swh.core.api import RPCClient
from swh.objstorage.constants import LiteralPrimaryHash
from swh.objstorage.exc import (
    Error,
    NoBackendsLeftError,
    ObjCorruptedError,
    ObjNotFoundError,
    ObjStorageAPIError,
)
from swh.objstorage.interface import ObjId, ObjStorageInterface
from swh.objstorage.objstorage import timed


class RemoteObjStorage(RPCClient):
    """Proxy to a remote object storage.

    This class allows to connect to an object storage server via
    http protocol.

    Attributes:
        url (string): The url of the server to connect. Must end
            with a '/'
        session: The session to send requests.

    """

    primary_hash: Optional[LiteralPrimaryHash] = None

    api_exception = ObjStorageAPIError
    reraise_exceptions = [
        ObjNotFoundError,
        Error,
        ObjCorruptedError,
        NoBackendsLeftError,
        PermissionError,
    ]
    backend_class = ObjStorageInterface
    name: str = "remote"

    def restore(self: ObjStorageInterface, content: bytes, obj_id: ObjId) -> None:
        return self.add(content, obj_id, check_presence=False)


# XXX Maybe there is a better way of doing this, but according the automagic
# way this class is built (via the MetaRPCClient metaclass), one cannot easily
# just overload the methods in the class definition.
RemoteObjStorage.get = timed(RemoteObjStorage.get)  # type: ignore
RemoteObjStorage.add = timed(RemoteObjStorage.add)  # type: ignore
RemoteObjStorage.__contains__ = timed(RemoteObjStorage.__contains__)  # type: ignore
