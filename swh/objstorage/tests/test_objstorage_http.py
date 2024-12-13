# Copyright (C) 2021-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
import requests
import requests_mock
from requests_mock.contrib import fixture

from swh.objstorage.exc import (
    NonIterableObjStorageError,
    ObjCorruptedError,
    ObjNotFoundError,
    ReadOnlyObjStorageError,
)
from swh.objstorage.factory import get_objstorage
from swh.objstorage.objstorage import objid_for_content

from .objstorage_testing import FIRST_OBJID


@pytest.fixture
def build_objstorage():
    """Build an HTTPReadOnlyObjStorage suitable for tests

    this instancaite 2 ObjStorage, one HTTPReadOnlyObjStorage (the "front" one
    being under test), and one InMemoryObjStorage (which actually stores the
    test content), and install a request mock fixture to route HTTP requests
    from the HTTPReadOnlyObjStorage to query the InMemoryStorage.

    Also fills the backend storage with a 100 objects.
    """
    sto_back = get_objstorage(cls="memory")
    objids = []
    for i in range(100):
        content = f"some content {i}".encode()
        obj_id = objid_for_content(content)
        objids.append(obj_id)
        sto_back.add(content, obj_id=obj_id)

    url = "http://127.0.0.1/content/"
    sto_front = get_objstorage(cls="http", url=url)
    mock = fixture.Fixture()
    mock.setUp()

    def get_cb(request, context):
        dirname, basename = request.path.rsplit("/", 1)
        primary_hash = bytes.fromhex(basename)
        back_objid = {sto_back.PRIMARY_HASH: primary_hash}
        if dirname == "/content" and back_objid in sto_back:
            return sto_back.get(back_objid)
        context.status_code = 404

    def head_cb(request, context):
        dirname, basename = request.path.rsplit("/", 1)
        primary_hash = bytes.fromhex(basename)
        back_objid = {sto_back.PRIMARY_HASH: primary_hash}
        if dirname != "/content" or back_objid not in sto_back:
            context.status_code = 404
            return b"Not Found"
        return b"Found"

    mock.register_uri(requests_mock.GET, requests_mock.ANY, content=get_cb)
    mock.register_uri(requests_mock.HEAD, requests_mock.ANY, content=head_cb)

    yield sto_front, sto_back, objids
    mock.cleanUp()


def test_http_objstorage(build_objstorage):
    sto_front, sto_back, objids = build_objstorage

    for objid in objids:
        assert objid in sto_front
        assert sto_front.get(objid) == sto_back.get(objid)
        assert sto_front.get(objid).decode().startswith("some content ")


def test_http_objstorage_missing(build_objstorage):
    sto_front, _, _ = build_objstorage

    assert FIRST_OBJID not in sto_front


def test_http_objstorage_get_missing(build_objstorage):
    sto_front, _, _ = build_objstorage

    with pytest.raises(ObjNotFoundError):
        sto_front.get(FIRST_OBJID)


def test_http_objstorage_check(build_objstorage):
    sto_front, sto_back, objids = build_objstorage
    for objid in objids:
        assert sto_front.check(objid) is None  # no Exception means OK

    # create an invalid object in the in-memory objstorage
    invalid_content = b"p0wn3d content"
    sto_back.add(invalid_content, FIRST_OBJID)

    # the http objstorage should report it as invalid
    with pytest.raises(ObjCorruptedError):
        sto_front.check(FIRST_OBJID)


def test_http_objstorage_read_only(build_objstorage):
    sto_front, sto_back, objids = build_objstorage

    content = b""
    obj_id = objid_for_content(content)
    with pytest.raises(ReadOnlyObjStorageError):
        sto_front.add(content, obj_id=obj_id)
    with pytest.raises(ReadOnlyObjStorageError):
        sto_front.restore(b"", obj_id=objid_for_content(b""))
    with pytest.raises(ReadOnlyObjStorageError):
        sto_front.delete(b"\x00" * 20)


def test_http_objstorage_not_iterable(build_objstorage):
    sto_front, _, _ = build_objstorage

    with pytest.raises(NonIterableObjStorageError):
        len(sto_front)
    with pytest.raises(NonIterableObjStorageError):
        iter(sto_front)


def test_http_cannonical_url():
    url = "http://127.0.0.1/content"
    sto = get_objstorage(cls="http", url=url)
    assert sto.root_path == url + "/"


def test_http_objstorage_download_url(build_objstorage):
    sto_front, _, objids = build_objstorage

    for objid in objids:
        assert objid in sto_front
        response = requests.get(sto_front.download_url(objid))
        assert response.text.startswith("some content ")
