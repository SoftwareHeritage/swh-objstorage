# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import io
import logging
from urllib.parse import urljoin, urlparse

import requests

from swh.model import hashutil
from swh.objstorage.exc import Error, ObjNotFoundError
from swh.objstorage.objstorage import (
    DEFAULT_LIMIT,
    ObjStorage,
    compressors,
    compute_hash,
    decompressors,
)

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.ERROR)


class WeedFiler(object):
    """Simple class that encapsulates access to a seaweedfs filer service.

    TODO: handle errors
    """

    def __init__(self, url):
        self.url = url

    def get(self, remote_path):
        url = urljoin(self.url, remote_path)
        LOGGER.debug("Get file %s", url)
        return requests.get(url).content

    def exists(self, remote_path):
        url = urljoin(self.url, remote_path)
        LOGGER.debug("Check file %s", url)
        return requests.head(url).status_code == 200

    def put(self, fp, remote_path):
        url = urljoin(self.url, remote_path)
        LOGGER.debug("Put file %s", url)
        return requests.post(url, files={"file": fp})

    def delete(self, remote_path):
        url = urljoin(self.url, remote_path)
        LOGGER.debug("Delete file %s", url)
        return requests.delete(url)

    def list(self, dir, last_file_name=None, limit=DEFAULT_LIMIT):
        """list sub folders and files of @dir. show a better look if you turn on

        returns a dict of "sub-folders and files"

        """
        d = dir if dir.endswith("/") else (dir + "/")
        url = urljoin(self.url, d)
        headers = {"Accept": "application/json"}
        params = {"limit": limit}
        if last_file_name:
            params["lastFileName"] = last_file_name

        LOGGER.debug("List directory %s", url)
        rsp = requests.get(url, params=params, headers=headers)
        if rsp.ok:
            return rsp.json()
        else:
            LOGGER.error('Error listing "%s". [HTTP %d]' % (url, rsp.status_code))


class WeedObjStorage(ObjStorage):
    """ObjStorage with seaweedfs abilities, using the Filer API.

    https://github.com/chrislusf/seaweedfs/wiki/Filer-Server-API
    """

    def __init__(self, url="http://127.0.0.1:8888/swh", compression=None, **kwargs):
        super().__init__(**kwargs)
        self.wf = WeedFiler(url)
        self.root_path = urlparse(url).path
        self.compression = compression

    def check_config(self, *, check_write):
        """Check the configuration for this object storage"""
        # FIXME: hopefully this blew up during instantiation
        return True

    def __contains__(self, obj_id):
        return self.wf.exists(self._path(obj_id))

    def __iter__(self):
        """ Iterate over the objects present in the storage

        Warning: Iteration over the contents of a cloud-based object storage
        may have bad efficiency: due to the very high amount of objects in it
        and the fact that it is remote, get all the contents of the current
        object storage may result in a lot of network requests.

        You almost certainly don't want to use this method in production.
        """
        obj_id = last_obj_id = None
        while True:
            for obj_id in self.list_content(last_obj_id=last_obj_id):
                yield obj_id
            if last_obj_id == obj_id:
                break
            last_obj_id = obj_id

    def __len__(self):
        """Compute the number of objects in the current object storage.

        Warning: this currently uses `__iter__`, its warning about bad
        performance applies.

        Returns:
            number of objects contained in the storage.

        """
        return sum(1 for i in self)

    def add(self, content, obj_id=None, check_presence=True):
        if obj_id is None:
            # Checksum is missing, compute it on the fly.
            obj_id = compute_hash(content)

        if check_presence and obj_id in self:
            return obj_id

        def compressor(data):
            comp = compressors[self.compression]()
            for chunk in data:
                yield comp.compress(chunk)
            yield comp.flush()

        if isinstance(content, bytes):
            content = [content]

        # XXX should handle streaming correctly...
        self.wf.put(io.BytesIO(b"".join(compressor(content))), self._path(obj_id))
        return obj_id

    def restore(self, content, obj_id=None):
        return self.add(content, obj_id, check_presence=False)

    def get(self, obj_id):
        try:
            obj = self.wf.get(self._path(obj_id))
        except Exception:
            raise ObjNotFoundError(obj_id)

        d = decompressors[self.compression]()
        ret = d.decompress(obj)
        if d.unused_data:
            hex_obj_id = hashutil.hash_to_hex(obj_id)
            raise Error("Corrupt object %s: trailing data found" % hex_obj_id)
        return ret

    def check(self, obj_id):
        # Check the content integrity
        obj_content = self.get(obj_id)
        content_obj_id = compute_hash(obj_content)
        if content_obj_id != obj_id:
            raise Error(obj_id)

    def delete(self, obj_id):
        super().delete(obj_id)  # Check delete permission
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)
        self.wf.delete(self._path(obj_id))
        return True

    def list_content(self, last_obj_id=None, limit=DEFAULT_LIMIT):
        if last_obj_id:
            last_obj_id = hashutil.hash_to_hex(last_obj_id)
        resp = self.wf.list(self.root_path, last_obj_id, limit)
        if resp is not None:
            entries = resp["Entries"]
            if entries:
                for obj in entries:
                    if obj is not None:
                        bytehex = obj["FullPath"].rsplit("/", 1)[-1]
                        yield hashutil.bytehex_to_hash(bytehex.encode())

    # internal methods
    def _put_object(self, content, obj_id):
        """Create an object in the cloud storage.

        Created object will contain the content and be referenced by
        the given id.

        """

        def compressor(data):
            comp = compressors[self.compression]()
            for chunk in data:
                yield comp.compress(chunk)
            yield comp.flush()

        if isinstance(content, bytes):
            content = [content]
        self.wf.put(io.BytesIO(b"".join(compressor(content))), self._path(obj_id))

    def _path(self, obj_id):
        return hashutil.hash_to_hex(obj_id)
