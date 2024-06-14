# Copyright (C) 2019-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import timedelta
import io
from itertools import islice
import logging
from typing import Iterator, Optional
from urllib.parse import urlparse

from typing_extensions import Literal

from swh.objstorage.backends.pathslicing import PathSlicer
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.interface import ObjId
from swh.objstorage.objstorage import (
    DEFAULT_LIMIT,
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

    PRIMARY_HASH: Literal["sha1", "sha256"] = "sha1"
    name: str = "seaweedfs"

    def __init__(
        self,
        *,
        url: str = "",
        compression: CompressionFormat = "none",
        slicing: str = "",
        pool_maxsize: int = 100,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.compression = compression
        self.wf = HttpFiler(url, pool_maxsize=pool_maxsize)
        self.root_path = urlparse(url).path
        if not self.root_path.endswith("/"):
            self.root_path += "/"
        self.slicer = PathSlicer(self.root_path, slicing)

    def check_config(self, *, check_write):
        """Check the configuration for this object storage"""
        # FIXME: hopefully this blew up during instantiation
        return True

    @timed
    def __contains__(self, obj_id: ObjId) -> bool:
        return self.wf.exists(self._path(obj_id))

    def __iter__(self) -> Iterator[ObjId]:
        """Iterate over the objects present in the storage

        Warning: Iteration over the contents of a cloud-based object storage
        may have bad efficiency: due to the very high amount of objects in it
        and the fact that it is remote, get all the contents of the current
        object storage may result in a lot of network requests.

        You almost certainly don't want to use this method in production.
        """
        for obj_id in self.list_content(limit=None):
            assert obj_id
            yield obj_id

    def __len__(self):
        """Compute the number of objects in the current object storage.

        Warning: this currently uses `__iter__`, its warning about bad
        performance applies.

        Returns:
            number of objects contained in the storage.

        """
        return sum(1 for i in self)

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

        return self.decompress(obj, objid_to_default_hex(obj_id))

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

    def list_content(
        self,
        last_obj_id: Optional[ObjId] = None,
        limit: Optional[int] = DEFAULT_LIMIT,
    ) -> Iterator[ObjId]:
        if last_obj_id:
            objpath = self._path(last_obj_id)
            startdir, lastfilename = objpath.rsplit("/", 1)
        else:
            startdir = self.root_path
            lastfilename = None
        for fname in islice(
            self.wf.iterfiles(startdir, last_file_name=lastfilename), limit
        ):
            bytehex = fname.rsplit("/", 1)[-1]
            if self.PRIMARY_HASH == "sha1":
                yield {"sha1": bytes.fromhex(bytehex)}
            elif self.PRIMARY_HASH == "sha256":
                yield {"sha256": bytes.fromhex(bytehex)}
            else:
                raise ValueError(f"Unknown primary hash {self.PRIMARY_HASH}")

    # internal methods
    def _path(self, obj_id: ObjId):
        """Compute the backend path for the given obj id

        Given an object is, return the path part of the url to query the
        backend seaweedfs filer service with, according the configured path
        slicing.

        """
        return self.slicer.get_path(objid_to_default_hex(obj_id))
