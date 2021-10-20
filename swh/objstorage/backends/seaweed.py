# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import io
from itertools import islice
import logging
import os
from typing import Iterator
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

    Objects are expected to be in a single directory.
    TODO: handle errors
    """

    def __init__(self, url):
        if not url.endswith("/"):
            url = url + "/"
        self.url = url
        self.baseurl = urljoin(url, "/")
        self.basepath = urlparse(url).path

        self.session = requests.Session()
        self.session.headers["Accept"] = "application/json"

        self.batchsize = DEFAULT_LIMIT

    def build_url(self, path):
        assert path == self.basepath or path.startswith(self.basepath)
        return urljoin(self.baseurl, path)

    def get(self, remote_path):
        url = self.build_url(remote_path)
        LOGGER.debug("Get file %s", url)
        resp = self.session.get(url)
        resp.raise_for_status()
        return resp.content

    def exists(self, remote_path):
        url = self.build_url(remote_path)
        LOGGER.debug("Check file %s", url)
        return self.session.head(url).status_code == 200

    def put(self, fp, remote_path):
        url = self.build_url(remote_path)
        LOGGER.debug("Put file %s", url)
        return self.session.post(url, files={"file": fp})

    def delete(self, remote_path):
        url = self.build_url(remote_path)
        LOGGER.debug("Delete file %s", url)
        return self.session.delete(url)

    def iterfiles(self, last_file_name: str = "") -> Iterator[str]:
        """yield absolute file names

        Args:
            last_file_name: if given, starts from the file just after; must
                be basename.

        Yields:
            absolute file names

        """
        for entry in self._iter_dir(last_file_name):
            fullpath = entry["FullPath"]
            if entry["Mode"] & 1 << 31:  # it's a directory, recurse
                # see https://pkg.go.dev/io/fs#FileMode
                yield from self.iterfiles(fullpath)
            else:
                yield fullpath

    def _iter_dir(self, last_file_name: str = ""):
        params = {"limit": self.batchsize}
        if last_file_name:
            params["lastFileName"] = last_file_name

        LOGGER.debug("List directory %s", self.url)
        while True:
            rsp = self.session.get(self.url, params=params)
            if rsp.ok:
                dircontent = rsp.json()
                if dircontent["Entries"]:
                    yield from dircontent["Entries"]
                if not dircontent["ShouldDisplayLoadMore"]:
                    break
                params["lastFileName"] = dircontent["LastFileName"]

            else:
                LOGGER.error(
                    'Error listing "%s". [HTTP %d]' % (self.url, rsp.status_code)
                )
                break


class WeedObjStorage(ObjStorage):
    """ObjStorage with seaweedfs abilities, using the Filer API.

    https://github.com/chrislusf/seaweedfs/wiki/Filer-Server-API
    """

    def __init__(self, url="http://127.0.0.1:8888/swh", compression=None, **kwargs):
        super().__init__(**kwargs)
        self.wf = WeedFiler(url)
        self.root_path = urlparse(url).path
        if not self.root_path.endswith("/"):
            self.root_path += "/"
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
            objid = hashutil.hash_to_hex(last_obj_id)
            lastfilename = objid
        else:
            lastfilename = None
        for fname in islice(self.wf.iterfiles(last_file_name=lastfilename), limit):
            bytehex = fname.rsplit("/", 1)[-1]
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
        return os.path.join(self.wf.basepath, hashutil.hash_to_hex(obj_id))
