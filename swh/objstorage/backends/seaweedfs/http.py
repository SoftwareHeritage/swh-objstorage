# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from typing import Iterator
from urllib.parse import urljoin, urlparse

import requests

from swh.objstorage.objstorage import DEFAULT_LIMIT

LOGGER = logging.getLogger(__name__)


class HttpFiler:
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
                LOGGER.error('Error listing "%s". [HTTP %d]', self.url, rsp.status_code)
                break
