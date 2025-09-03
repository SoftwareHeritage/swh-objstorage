# Copyright (C) 2021-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import timedelta
import logging
from typing import Dict, Optional
from urllib.parse import urljoin

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
LOGGER.setLevel(logging.ERROR)


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

    """

    primary_hash: LiteralPrimaryHash = "sha1"
    name: str = "http"

    def __init__(
        self, url=None, compression: CompressionFormat | None = None, **kwargs
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
