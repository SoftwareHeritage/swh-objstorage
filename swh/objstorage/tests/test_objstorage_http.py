# Copyright (C) 2021-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import re

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


@pytest.fixture(params=("sha1", "sha256"))
def objstorages(request, requests_mock, contents, obj_ids):
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

    url = "http://127.0.0.1/content/"
    sto_front = get_objstorage(cls="http", url=url, primary_hash=request.param)

    def get_cb(request, context):
        dirname, basename = request.path.rsplit("/", 1)
        primary_hash = bytes.fromhex(basename)
        back_objid = {sto_back.primary_hash: primary_hash}
        if dirname == "/content" and back_objid in sto_back:
            return sto_back.get(back_objid)
        context.status_code = 404

    def head_cb(request, context):
        dirname, basename = request.path.rsplit("/", 1)
        primary_hash = bytes.fromhex(basename)
        back_objid = {sto_back.primary_hash: primary_hash}
        if dirname != "/content" or back_objid not in sto_back:
            context.status_code = 404
            return b"Not Found"
        return b"Found"

    matcher = re.compile(f"{url}*")
    requests_mock.get(matcher, content=get_cb)
    requests_mock.head(matcher, content=head_cb)

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
