# Copyright (C) 2021-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random
import re

from aioresponses import CallbackResult
import pytest
import requests

from swh.objstorage.exc import (
    ObjCorruptedError,
    ObjNotFoundError,
    ReadOnlyObjStorageError,
)
from swh.objstorage.factory import get_objstorage
from swh.objstorage.objstorage import objid_for_content

from .objstorage_testing import FIRST_OBJID


@pytest.fixture
def contents():
    return [f"some content {i}".encode() for i in range(100)]


@pytest.fixture
def obj_ids(contents):
    return [objid_for_content(content) for content in contents]


URL = "http://127.0.0.1/content/"


def mock_http_responses(sto_back, requests_mock=None, aioresponses=None):
    def process_get(path):
        dirname, basename = path.rsplit("/", 1)
        primary_hash = bytes.fromhex(basename)
        back_objid = {sto_back.primary_hash: primary_hash}
        if dirname == "/content" and back_objid in sto_back:
            return (200, sto_back.get(back_objid))
        return (404, b"")

    def process_head(path):
        dirname, basename = path.rsplit("/", 1)
        primary_hash = bytes.fromhex(basename)
        back_objid = {sto_back.primary_hash: primary_hash}
        if dirname != "/content" or back_objid not in sto_back:
            return (404, b"Not Found")
        return (200, b"Found")

    def sync_request_cb(process_request):
        def cb(request, context):
            status, body = process_request(request.path)
            if status == 200:
                return body
            context.status_code = status

        return cb

    def async_request_cb(process_request):
        def cb(url, **kwargs):
            status, body = process_request(url.path)
            return CallbackResult(status=status, body=body)

        return cb

    matcher = re.compile(f"^{URL}.*$")
    if requests_mock:
        requests_mock.get(matcher, content=sync_request_cb(process_get))
        requests_mock.head(matcher, content=sync_request_cb(process_head))
    if aioresponses:
        aioresponses.get(matcher, callback=async_request_cb(process_get), repeat=True)


@pytest.fixture(params=("sha1", "sha256"))
def objstorages(request, requests_mock, contents, obj_ids, aioresponses):
    """Build an HTTPReadOnlyObjStorage suitable for tests

    this instancaite 2 ObjStorage, one HTTPReadOnlyObjStorage (the "front" one
    being under test), and one InMemoryObjStorage (which actually stores the
    test content), and install a request mock fixture to route HTTP requests
    from the HTTPReadOnlyObjStorage to query the InMemoryStorage.

    Also fills the backend storage with a 100 objects.
    """
    sto_back = get_objstorage(cls="memory", primary_hash=request.param)
    for content, obj_id in zip(contents, obj_ids):
        sto_back.add(content, obj_id=obj_id)

    sto_front = get_objstorage(cls="http", url=URL, primary_hash=request.param)

    mock_http_responses(sto_back, requests_mock, aioresponses)

    yield sto_front, sto_back


def test_http_objstorage(objstorages, obj_ids):
    sto_front, sto_back = objstorages

    for obj_id in obj_ids:
        assert obj_id in sto_front
        assert sto_front.get(obj_id) == sto_back.get(obj_id)
        assert sto_front.get(obj_id).decode().startswith("some content ")


def test_http_objstorage_missing(objstorages):
    sto_front, _ = objstorages

    assert FIRST_OBJID not in sto_front


def test_http_objstorage_get_missing(objstorages):
    sto_front, _ = objstorages

    with pytest.raises(ObjNotFoundError):
        sto_front.get(FIRST_OBJID)


def test_http_objstorage_check(objstorages, obj_ids):
    sto_front, sto_back = objstorages
    for objid in obj_ids:
        assert sto_front.check(objid) is None  # no Exception means OK

    # create an invalid object in the in-memory objstorage
    invalid_content = b"p0wn3d content"
    sto_back.add(invalid_content, FIRST_OBJID)

    # the http objstorage should report it as invalid
    with pytest.raises(ObjCorruptedError):
        sto_front.check(FIRST_OBJID)


def test_http_objstorage_read_only(objstorages):
    sto_front, _ = objstorages

    content = b""
    obj_id = objid_for_content(content)
    with pytest.raises(ReadOnlyObjStorageError):
        sto_front.add(content, obj_id=obj_id)
    with pytest.raises(ReadOnlyObjStorageError):
        sto_front.restore(b"", obj_id=objid_for_content(b""))
    with pytest.raises(ReadOnlyObjStorageError):
        sto_front.delete(b"\x00" * 20)


def test_http_cannonical_url():
    url = "http://127.0.0.1/content"
    sto = get_objstorage(cls="http", url=url)
    assert sto.root_path == url + "/"


def test_http_objstorage_download_url(objstorages, obj_ids):
    sto_front, _ = objstorages

    for obj_id in obj_ids:
        assert obj_id in sto_front
        response = requests.get(sto_front.download_url(obj_id))
        assert response.text.startswith("some content ")


def test_http_objstorage_get_batch(objstorages, contents, obj_ids):
    sto_front, sto_back = objstorages
    contents_front = list(sto_front.get_batch(obj_ids))
    contents_back = list(sto_back.get_batch(obj_ids))
    assert contents_front == contents_back == contents


def test_http_objstorage_get_batch_exception(
    objstorages, contents, obj_ids, aioresponses
):
    sto_front, sto_back = objstorages
    idx = random.randint(0, len(obj_ids) - 1)
    aioresponses.clear()
    aioresponses.get(sto_front._path(obj_ids[idx]), exception=Exception("error"))
    mock_http_responses(sto_back, aioresponses=aioresponses)

    contents_front = list(sto_front.get_batch(obj_ids))
    assert contents_front[idx] is None
    assert all(contents_front[i] is not None for i in range(len(obj_ids)) if i != idx)


def test_http_objstorage_get_batch_unknown_contents(objstorages):
    sto_front, sto_back = objstorages

    unknown_objids = [objid_for_content(f"unknown {i}".encode()) for i in range(10)]

    contents_front = list(sto_front.get_batch(unknown_objids))
    contents_back = list(sto_back.get_batch(unknown_objids))
    assert contents_front == contents_back == [None] * len(unknown_objids)
