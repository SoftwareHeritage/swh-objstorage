# Copyright (C) 2019-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import timedelta
import io
import logging
from typing import Optional
from urllib.parse import urlparse

from swh.objstorage.backends.pathslicing import PathSlicer
from swh.objstorage.constants import LiteralPrimaryHash
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.interface import ObjId
from swh.objstorage.objstorage import (
    CompressionFormat,
    ObjStorage,
    objid_to_default_hex,
    timed,
)

from .http import HttpFiler

LOGGER = logging.getLogger(__name__)


class SeaweedFilerObjStorage(ObjStorage):
    """ObjStorage with seaweedfs abilities, using the Filer API.

    https://github.com/chrislusf/seaweedfs/wiki/Filer-Server-API
    """

    primary_hash: LiteralPrimaryHash = "sha1"
    name: str = "seaweedfs"

    def __init__(
        self,
        *,
        url: str = "",
        compression: CompressionFormat | None = None,
        slicing: str = "",
        pool_maxsize: int = 100,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if compression is None:
            LOGGER.warning(
                "Deprecated: compression is undefined. "
                "Defaulting to none, but please set it explicitly."
            )
            compression = "none"
        self.compression = compression
        self.wf = HttpFiler(url, pool_maxsize=pool_maxsize)
        self.root_path = urlparse(url).path
        if not self.root_path.endswith("/"):
            self.root_path += "/"
        self.slicer = PathSlicer(self.root_path, slicing, self.primary_hash)

    def check_config(self, *, check_write):
        """Check the configuration for this object storage"""
        # FIXME: hopefully this blew up during instantiation
        return True

    @timed
    def __contains__(self, obj_id: ObjId) -> bool:
        return self.wf.exists(self._path(obj_id))

    @timed
    def add(self, content: bytes, obj_id: ObjId, check_presence: bool = True) -> None:
        if check_presence and obj_id in self:
            return

        self.wf.put(io.BytesIO(self.compress(content)), self._path(obj_id))

    @timed
    def restore(self, content: bytes, obj_id: ObjId) -> None:
        return self.add(content, obj_id, check_presence=False)

    @timed
    def get(self, obj_id: ObjId) -> bytes:
        try:
            obj = self.wf.get(self._path(obj_id))
        except Exception as exc:
            LOGGER.info("Failed to get object %s: %r", self._path(obj_id), exc)
            raise ObjNotFoundError(obj_id)

        return self.decompress(obj, objid_to_default_hex(obj_id, self.primary_hash))

    def download_url(
        self,
        obj_id: ObjId,
        content_disposition: Optional[str] = None,
        expiry: Optional[timedelta] = None,
    ) -> Optional[str]:
        path = self._path(obj_id)
        if not self.wf.exists(path):
            raise ObjNotFoundError(obj_id)
        return self.wf.build_url(path)

    def delete(self, obj_id: ObjId):
        super().delete(obj_id)  # Check delete permission
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)
        self.wf.delete(self._path(obj_id))
        return True

    # internal methods
    def _path(self, obj_id: ObjId):
        """Compute the backend path for the given obj id

        Given an object is, return the path part of the url to query the
        backend seaweedfs filer service with, according the configured path
        slicing.

        """
        return self.slicer.get_path(objid_to_default_hex(obj_id, self.primary_hash))
