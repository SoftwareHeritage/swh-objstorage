# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from urllib.parse import urljoin

import requests

from swh.model import hashutil
from swh.objstorage import exc
from swh.objstorage.objstorage import (
    DEFAULT_LIMIT,
    ObjStorage,
    compute_hash,
    decompressors,
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

    def __init__(self, url=None, compression=None, **kwargs):
        super().__init__(**kwargs)
        self.session = requests.sessions.Session()
        self.root_path = url
        if not self.root_path.endswith("/"):
            self.root_path += "/"
        self.compression = compression

    def check_config(self, *, check_write):
        """Check the configuration for this object storage"""
        return True

    def __contains__(self, obj_id):
        resp = self.session.head(self._path(obj_id))
        return resp.status_code == 200

    def __iter__(self):
        raise exc.NonIterableObjStorage("__iter__")

    def __len__(self):
        raise exc.NonIterableObjStorage("__len__")

    def add(self, content, obj_id=None, check_presence=True):
        raise exc.ReadOnlyObjStorage("add")

    def delete(self, obj_id):
        raise exc.ReadOnlyObjStorage("delete")

    def restore(self, content, obj_id=None):
        raise exc.ReadOnlyObjStorage("restore")

    def list_content(self, last_obj_id=None, limit=DEFAULT_LIMIT):
        raise exc.NonIterableObjStorage("__len__")

    def get(self, obj_id):
        try:
            resp = self.session.get(self._path(obj_id))
            resp.raise_for_status()
        except Exception:
            raise exc.ObjNotFoundError(obj_id)

        ret: bytes = resp.content
        if self.compression:
            d = decompressors[self.compression]()
            ret = d.decompress(ret)
            if d.unused_data:
                hex_obj_id = hashutil.hash_to_hex(obj_id)
                raise exc.Error("Corrupt object %s: trailing data found" % hex_obj_id)
        return ret

    def check(self, obj_id):
        # Check the content integrity
        obj_content = self.get(obj_id)
        content_obj_id = compute_hash(obj_content)
        if content_obj_id != obj_id:
            raise exc.Error(obj_id)

    def _path(self, obj_id):
        return urljoin(self.root_path, hashutil.hash_to_hex(obj_id))
