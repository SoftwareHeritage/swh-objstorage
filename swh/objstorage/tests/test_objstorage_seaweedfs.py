# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from itertools import dropwhile, islice
import json
import os
import unittest
from urllib.parse import urlparse

from requests.utils import get_encoding_from_headers
import requests_mock
from requests_mock.contrib import fixture

from swh.objstorage.exc import Error
from swh.objstorage.factory import get_objstorage
from swh.objstorage.objstorage import decompressors
from swh.objstorage.tests.objstorage_testing import ObjStorageTestFixture


class FilerRequestsMock:
    """This is a requests_mock based mock for the seaweedfs Filer API

    It does not implement the whole API, only the parts required to make the
    WeedFiler (used by WeedObjStorage) work.

    It stores the files in a dict.
    """

    MODE_DIR = 0o20000000771
    MODE_FILE = 0o660

    def __init__(self, baseurl):
        self.baseurl = baseurl
        self.basepath = urlparse(baseurl).path
        self.content = {}
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
        if path.startswith(self.basepath):
            return os.path.relpath(path, self.basepath)

    def head_cb(self, request, context):
        relpath = self.relpath(request.path)
        if relpath == "." or relpath in self.content:
            return b"Found"  # ok, found it
        context.status_code = 404
        return b"Not Found"

    def get_cb(self, request, context):
        if self.head_cb(request, context) == b"Not Found":
            return
        relpath = self.relpath(request.path)
        if relpath == ".":
            if "limit" in request.qs:
                limit = int(request.qs["limit"][0])
                assert limit > 0
            else:
                limit = None

            items = sorted(self.content.items())
            if items and "lastfilename" in request.qs:
                lastfilename = request.qs["lastfilename"][0]
                if lastfilename:
                    # exclude all filenames up to lastfilename
                    items = dropwhile(lambda kv: kv[0] <= lastfilename, items)

            if limit:
                # +1 to easily detect if there are more
                items = islice(items, limit + 1)

            entries = [
                {"FullPath": os.path.join(request.path, fname), "Mode": self.MODE_FILE,}
                for fname, obj in items
            ]

            thereismore = False
            if limit and len(entries) > limit:
                entries = entries[:limit]
                thereismore = True

            if entries:
                lastfilename = entries[-1]["FullPath"].split("/")[-1]
            else:
                lastfilename = None
            text = json.dumps(
                {
                    "Path": request.path,
                    "Limit": limit,
                    "LastFileName": lastfilename,
                    "ShouldDisplayLoadMore": thereismore,
                    "Entries": entries,
                }
            )
            encoding = get_encoding_from_headers(request.headers) or "utf-8"
            return text.encode(encoding)
        else:
            return self.content[relpath]

    def post_cb(self, request, context):
        from requests_toolbelt.multipart import decoder

        multipart_data = decoder.MultipartDecoder(
            request.body, request.headers["content-type"]
        )
        part = multipart_data.parts[0]
        self.content[self.relpath(request.path)] = part.content

    def delete_cb(self, request, context):
        del self.content[self.relpath(request.path)]


class TestWeedObjStorage(ObjStorageTestFixture, unittest.TestCase):
    compression = "none"
    url = "http://127.0.0.1/test/"

    def setUp(self):
        super().setUp()
        self.storage = get_objstorage(
            cls="seaweedfs", url=self.url, compression=self.compression
        )
        self.mock = FilerRequestsMock(baseurl=self.url)

    def test_compression(self):
        content, obj_id = self.hash_content(b"test compression")
        self.storage.add(content, obj_id=obj_id)

        raw_content = self.storage.wf.get(self.storage._path(obj_id))

        d = decompressors[self.compression]()
        assert d.decompress(raw_content) == content
        assert d.unused_data == b""

    def test_trailing_data_on_stored_blob(self):
        content, obj_id = self.hash_content(b"test content without garbage")
        self.storage.add(content, obj_id=obj_id)

        self.mock.content[obj_id.hex()] += b"trailing garbage"

        if self.compression == "none":
            with self.assertRaises(Error) as e:
                self.storage.check(obj_id)
        else:
            with self.assertRaises(Error) as e:
                self.storage.get(obj_id)
            assert "trailing data" in e.exception.args[0]


class TestWeedObjStorageWithCompression(TestWeedObjStorage):
    compression = "lzma"


class TestWeedObjStorageWithSmallBatch(TestWeedObjStorage):
    def setUp(self):
        super().setUp()
        self.storage.wf.batchsize = 1


class TestWeedObjStorageWithNoPath(TestWeedObjStorage):
    url = "http://127.0.0.1/"
