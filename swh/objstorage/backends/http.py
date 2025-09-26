# Copyright (C) 2021-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio
from datetime import timedelta
import logging
from typing import Dict, Iterable, Iterator, List, Optional
from urllib.parse import urljoin

import aiohttp
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from swh.model import hashutil
from swh.objstorage.constants import LiteralPrimaryHash
from swh.objstorage.exc import ObjNotFoundError, ReadOnlyObjStorageError
from swh.objstorage.interface import HashDict
from swh.objstorage.objstorage import (
    CompressionFormat,
    ObjStorage,
    objid_to_default_hex,
    timed,
)

LOGGER = logging.getLogger(__name__)


class HTTPReadOnlyObjStorage(ObjStorage):
    """Simple ObjStorage retrieving objects from an HTTP server.

    For example, can be used to retrieve objects from S3::

      objstorage:
        cls: http
        url: https://softwareheritage.s3.amazonaws.com/content/
        compression: gzip

    Retry strategy can be defined via the 'retry' configuration, e.g.::

      objstorage:
        cls: http
        url: https://softwareheritage.s3.amazonaws.com/content/
        compression: gzip
        retry:
          total: 5
          backoff_factor: 0.2
          status_forcelist:
            - 404
            - 500

    See
    https://urllib3.readthedocs.io/en/stable/reference/urllib3.util.html#urllib3.util.Retry
    for more details on the possible configuration entries.

    The :meth:`get_batch` method is implemented with ``aiohttp`` to improve the performance
    of object downloads. The maximum number of simultaneous connections can be set using
    the ``batch_max_connections`` parameter of that class (default to 100). The maximum
    number of simultaneous connections to the same host can be set using the
    ``batch_max_connections_per_host`` parameter of that class (default to 0 for no limit).
    """

    primary_hash: LiteralPrimaryHash = "sha1"
    name: str = "http"

    def __init__(
        self,
        url=None,
        compression: CompressionFormat | None = None,
        batch_max_connections: int = 100,
        batch_max_connections_per_host: int = 0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.session = Session()
        self.root_path = url
        if not self.root_path.endswith("/"):
            self.root_path += "/"
        if compression is None:
            LOGGER.warning(
                "Deprecated: compression is undefined. "
                "Defaulting to none, but please set it explicitly."
            )
            compression = "none"
        self.compression = compression
        self.batch_max_connections = batch_max_connections
        self.batch_max_connections_per_host = batch_max_connections_per_host
        retry: Optional[Dict] = kwargs.get("retry")
        if retry is not None:
            self.retries_cfg = Retry(**retry)
            self.session.mount(
                self.root_path, HTTPAdapter(max_retries=self.retries_cfg)
            )

    def check_config(self, *, check_write):
        """Check the configuration for this object storage"""
        return check_write is False

    @timed
    def __contains__(self, obj_id: HashDict) -> bool:
        resp = self.session.head(self._path(obj_id))
        return resp.status_code == 200

    @timed
    def add(
        self, content: bytes, obj_id: HashDict, check_presence: bool = True
    ) -> None:
        raise ReadOnlyObjStorageError("add")

    def delete(self, obj_id: HashDict):
        raise ReadOnlyObjStorageError("delete")

    def restore(self, content: bytes, obj_id: HashDict) -> None:
        raise ReadOnlyObjStorageError("restore")

    @timed
    def get(self, obj_id: HashDict) -> bytes:
        try:
            resp = self.session.get(self._path(obj_id))
            resp.raise_for_status()
        except Exception:
            raise ObjNotFoundError(obj_id)
        return self.decompress(
            resp.content, objid_to_default_hex(obj_id, self.primary_hash)
        )

    @timed
    def get_batch(self, obj_ids: Iterable[HashDict]) -> Iterator[Optional[bytes]]:
        return iter(asyncio.run(self._contents_get(list(obj_ids))))

    def download_url(
        self,
        obj_id: HashDict,
        content_disposition: Optional[str] = None,
        expiry: Optional[timedelta] = None,
    ) -> Optional[str]:
        return self._path(obj_id)

    def _hash(self, obj_id: HashDict) -> bytes:
        return obj_id[self.primary_hash]

    def _path(self, obj_id):
        return urljoin(self.root_path, hashutil.hash_to_hex(self._hash(obj_id)))

    async def _content_get(
        self,
        obj_id: HashDict,
        session: aiohttp.ClientSession,
    ) -> Optional[bytes]:
        try:
            url = self._path(obj_id)
            async with session.get(url) as response:
                response.raise_for_status()
                content = await response.read()
                return self.decompress(
                    content, objid_to_default_hex(obj_id, self.primary_hash)
                )
        except Exception as e:
            LOGGER.debug(
                "Unable to fetch or process content from URL %s due to %s.", url, str(e)
            )
        return None

    async def _contents_get(self, obj_ids: List[HashDict]) -> List[Optional[bytes]]:
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                limit=self.batch_max_connections,
                limit_per_host=self.batch_max_connections_per_host,
            )
        ) as session:
            return await asyncio.gather(
                *(self._content_get(obj_id, session) for obj_id in obj_ids)
            )
