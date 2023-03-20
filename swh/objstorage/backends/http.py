# Copyright (C) 2021-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import timedelta
import logging
from typing import Iterator, Optional
from urllib.parse import urljoin

import requests

from swh.model import hashutil
from swh.objstorage.constants import ID_HASH_ALGO
from swh.objstorage.exc import (
    NonIterableObjStorage,
    ObjNotFoundError,
    ReadOnlyObjStorage,
)
from swh.objstorage.interface import CompositeObjId, ObjId
from swh.objstorage.objstorage import (
    DEFAULT_LIMIT,
    CompressionFormat,
    ObjStorage,
    objid_to_default_hex,
)

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.ERROR)


class HTTPReadOnlyObjStorage(ObjStorage):
    """Simple ObjStorage retrieving objects from an HTTP server.

    For example, can be used to retrieve objects from S3:

    objstorage:
      cls: http
      url: https://softwareheritage.s3.amazonaws.com/content/
    """

    name: str = "http"

    def __init__(self, url=None, compression: CompressionFormat = "none", **kwargs):
        super().__init__(**kwargs)
        self.session = requests.sessions.Session()
        self.root_path = url
        if not self.root_path.endswith("/"):
            self.root_path += "/"
        self.compression = compression

    def check_config(self, *, check_write):
        """Check the configuration for this object storage"""
        return True

    def __contains__(self, obj_id: ObjId) -> bool:
        resp = self.session.head(self._path(obj_id))
        return resp.status_code == 200

    def __iter__(self) -> Iterator[CompositeObjId]:
        raise NonIterableObjStorage("__iter__")

    def __len__(self):
        raise NonIterableObjStorage("__len__")

    def add(self, content: bytes, obj_id: ObjId, check_presence: bool = True) -> None:
        raise ReadOnlyObjStorage("add")

    def delete(self, obj_id: ObjId):
        raise ReadOnlyObjStorage("delete")

    def restore(self, content: bytes, obj_id: ObjId) -> None:
        raise ReadOnlyObjStorage("restore")

    def list_content(
        self,
        last_obj_id: Optional[ObjId] = None,
        limit: Optional[int] = DEFAULT_LIMIT,
    ) -> Iterator[CompositeObjId]:
        raise NonIterableObjStorage("__len__")

    def get(self, obj_id: ObjId) -> bytes:
        try:
            resp = self.session.get(self._path(obj_id))
            resp.raise_for_status()
        except Exception:
            raise ObjNotFoundError(obj_id)

        return self.decompress(resp.content, objid_to_default_hex(obj_id))

    def download_url(
        self,
        obj_id: ObjId,
        content_disposition: Optional[str] = None,
        expiry: Optional[timedelta] = None,
    ) -> Optional[str]:
        return self._path(obj_id)

    def _hash(self, obj_id: ObjId) -> bytes:
        if isinstance(obj_id, dict):
            return obj_id[ID_HASH_ALGO]
        else:
            return obj_id

    def _path(self, obj_id):
        return urljoin(self.root_path, hashutil.hash_to_hex(self._hash(obj_id)))
