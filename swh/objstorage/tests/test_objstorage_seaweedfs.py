# Copyright (C) 2019-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import os
from urllib.parse import urlparse

import pytest
from requests.utils import get_encoding_from_headers
import requests_mock
from requests_mock.contrib import fixture

from swh.objstorage.backends.pathslicing import PathSlicer
from swh.objstorage.constants import ID_HEXDIGEST_LENGTH_BY_ALGO
from swh.objstorage.exc import ObjCorruptedError
from swh.objstorage.objstorage import (
    decompressors,
    objid_for_content,
    objid_to_default_hex,
)
from swh.objstorage.tests.objstorage_testing import ObjStorageTestFixture


class PathDict:
    """A dict-like object that handles "path-like" keys in a recursive dict
    structure.

    For example:

        >>> a = PathDict()
        >>> a['path/to/file'] = 'some file content'

    will create a dict structure (in self.data) like:

        >>> print(a.data)
        {'path': {'to': {'file': 'some file content'}}}
        >>> 'path/to/file' in a
        True

    This is a helper class for the FilerRequestsMock below.
    """

    def __init__(self):
        self.data = {}

    def __setitem__(self, key, value):
        if key.endswith("/"):
            raise ValueError("Nope")
        if key.startswith("/"):
            key = key[1:]
        path = key.split("/")
        resu = self.data
        for p in path[:-1]:
            resu = resu.setdefault(p, {})
        resu[path[-1]] = value

    def __getitem__(self, key):
        assert isinstance(key, str)
        if key == "/":
            return self.data

        if key.startswith("/"):
            key = key[1:]
        if key.endswith("/"):
            key = key[:-1]

        path = key.split("/")
        resu = self.data
        for p in path:
            resu = resu[p]
        return resu

    def __delitem__(self, key):
        if key.startswith("/"):
            key = key[1:]
        if key.endswith("/"):
            key = key[:-1]
        path = key.split("/")
        resu = self.data
        for p in path[:-1]:
            resu = resu.setdefault(p, {})
        del resu[path[-1]]

    def __contains__(self, key):
        if key == "/":
            # always consider we have the 'root' directory
            return True
        try:
            self[key]
            return True
        except KeyError:
            return False


class FilerRequestsMock:
    """This is a requests_mock based mock for the seaweedfs Filer API

    It does not implement the whole API, only the parts required to make the
    HttpFiler (used by SeaweedFilerObjStorage) work.

    It stores the files in a dict-based structure, eg. the file
    '0a/32/0a3245983255' will be stored in a dict like:

        {'0a': {'32': {'0a3245983255': b'content'}}}

    It uses the PathDict helper class to make it a bit easier to handle this
    dict structure.

    """

    MODE_DIR = 0o20000000771
    MODE_FILE = 0o660

    def __init__(self, url):
        self.url = url
        self.root_path = urlparse(url).path
        self.content = PathDict()
        self.requests_mock = fixture.Fixture()
        self.requests_mock.setUp()
        self.requests_mock.register_uri(
            requests_mock.GET, requests_mock.ANY, content=self.get_cb
        )
        self.requests_mock.register_uri(
            requests_mock.POST, requests_mock.ANY, content=self.post_cb
        )
        self.requests_mock.register_uri(
            requests_mock.HEAD, requests_mock.ANY, content=self.head_cb
        )
        self.requests_mock.register_uri(
            requests_mock.DELETE, requests_mock.ANY, content=self.delete_cb
        )

    def relpath(self, path):
        if path.startswith(self.root_path):
            return os.path.relpath(path, self.root_path)

    def head_cb(self, request, context):
        if request.path not in self.content:
            context.status_code = 404
            return b"Not Found"

    def get_cb(self, request, context):
        if self.head_cb(request, context) == b"Not Found":
            return

        if "limit" in request.qs:
            limit = int(request.qs["limit"][0])
            assert limit > 0
        else:
            limit = None

        if "lastfilename" in request.qs:
            lastfilename = request.qs["lastfilename"][0]
        else:
            lastfilename = None

        content = self.content[request.path]
        if isinstance(content, dict):
            # it's a directory; list its content
            entries = sorted(content.keys())
            if not entries:
                text = json.dumps(
                    {
                        "EmptyFolder": True,
                        "Entries": None,
                        "LastFileName": lastfilename,
                        "Limit": limit,
                        "Path": request.path,
                        "ShouldDisplayLoadMore": False,
                    }
                )
                encoding = get_encoding_from_headers(request.headers) or "utf-8"
                return text.encode(encoding)

            if lastfilename:
                # exclude all filenames up to lastfilename
                entries = [k for k in entries if k > lastfilename]

            thereismore = False
            if limit and len(entries) > limit:
                entries = entries[:limit]
                thereismore = True

            resp_entries = []
            for entry in entries:
                fullpath = os.path.join(request.path, entry)
                if isinstance(self.content[fullpath], dict):
                    mode = self.MODE_DIR
                else:
                    mode = self.MODE_FILE
                resp_entries.append(
                    {
                        "FullPath": fullpath,
                        "Mode": mode,
                    }
                )

            if resp_entries:
                lastfilename = resp_entries[-1]["FullPath"].split("/")[-1]
            else:
                lastfilename = None
            text = json.dumps(
                {
                    "Path": request.path,
                    "Limit": limit,
                    "LastFileName": lastfilename,
                    "ShouldDisplayLoadMore": thereismore,
                    "Entries": resp_entries,
                }
            )
            encoding = get_encoding_from_headers(request.headers) or "utf-8"
            return text.encode(encoding)
        else:
            # return the actual file content
            return content

    def post_cb(self, request, context):
        from requests_toolbelt.multipart import decoder

        multipart_data = decoder.MultipartDecoder(
            request.body, request.headers["content-type"]
        )
        part = multipart_data.parts[0]
        self.content[request.path] = part.content

    def delete_cb(self, request, context):
        del self.content[request.path]


class TestSeaweedObjStorage(ObjStorageTestFixture):
    compression = "none"
    url = "http://127.0.0.1/test/"
    slicing = ""

    @pytest.fixture
    def swh_objstorage_config(self):
        self.mock = FilerRequestsMock(url=self.url)
        return {
            "cls": "seaweedfs",
            "url": self.url,
            "compression": self.compression,
            "slicing": self.slicing,
        }

    def fill_objstorage(self, num_objects):
        # override default implelentation to speed things up a bit, shortcuting
        # the HTTP request path to put objects directly in the objstorage
        # mocker.

        path = self.storage._path
        all_ids = []
        for i in range(num_objects):
            content = b"content %d" % i
            obj_id = objid_for_content(content)
            self.mock.content[path(obj_id)] = self.storage.compress(content)
            all_ids.append(obj_id)
        all_ids.sort(key=lambda d: d[self.storage.PRIMARY_HASH])
        return all_ids

    def test_compression(self):
        content, obj_id = self.hash_content(b"test compression")
        self.storage.add(content, obj_id=obj_id)

        raw_content = self.storage.wf.get(self.storage._path(obj_id))
        if self.compression == "none":
            assert raw_content == content
        else:
            assert raw_content != content
        d = decompressors[self.compression]()
        assert d.decompress(raw_content) == content
        assert d.unused_data == b""

    def test_trailing_data_on_stored_blob(self):
        content, obj_id = self.hash_content(b"test content without garbage")
        self.storage.add(content, obj_id=obj_id)

        path = self.storage._path(obj_id)
        self.mock.content[path] += b"trailing garbage"

        with pytest.raises(ObjCorruptedError) as e:
            self.storage.check(obj_id)

        if self.compression != "none":
            assert "trailing data found" in e.value.args[0]

    def test_slicing(self):
        slicer = PathSlicer(
            urlparse(self.url).path, self.slicing, self.storage.PRIMARY_HASH
        )
        for i in range(ID_HEXDIGEST_LENGTH_BY_ALGO[self.storage.PRIMARY_HASH]):
            content, obj_id = self.hash_content(b"test slicing %i" % i)
            self.storage.add(content, obj_id=obj_id)
            hex_obj_id = objid_to_default_hex(obj_id, self.storage.PRIMARY_HASH)
            assert slicer.get_path(hex_obj_id) in self.mock.content


class TestSeaweedObjStorageWithCompression(TestSeaweedObjStorage):
    compression = "lzma"


class TestSeaweedObjStorageWithSlicing1(TestSeaweedObjStorage):
    num_objects = 12000
    slicing = "0:2"


class TestSeaweedObjStorageWithSlicing2(TestSeaweedObjStorage):
    num_objects = 12000
    slicing = "0:1/1:2"


class TestSeaweedObjStorageWithSmallBatch(TestSeaweedObjStorage):
    num_objects = 120

    @pytest.fixture(autouse=True)
    def batchsize(self, swh_objstorage):
        swh_objstorage.wf.batchsize = 1


class TestSeaweedObjStorageWithSlicing1AndSmallBatch(
    TestSeaweedObjStorageWithSmallBatch
):
    slicing = "0:2"


class TestSeaweedObjStorageWithSlicing2AndSmallBatch(
    TestSeaweedObjStorageWithSmallBatch
):
    slicing = "0:1/1:2"


class TestSeaweedObjStorageWithNoPath(TestSeaweedObjStorage):
    url = "http://127.0.0.1/"
