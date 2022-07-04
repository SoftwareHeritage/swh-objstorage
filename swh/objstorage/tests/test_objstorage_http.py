# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
import requests_mock
from requests_mock.contrib import fixture

from swh.objstorage import exc
from swh.objstorage.factory import get_objstorage
from swh.objstorage.objstorage import compute_hash


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
        obj_id = compute_hash(content)
        objids.append(obj_id)
        sto_back.add(content, obj_id=obj_id)

    url = "http://127.0.0.1/content/"
    sto_front = get_objstorage(cls="http", url=url)
    mock = fixture.Fixture()
    mock.setUp()

    def get_cb(request, context):
        dirname, basename = request.path.rsplit("/", 1)
        objid = bytes.fromhex(basename)
        if dirname == "/content" and objid in sto_back:
            return sto_back.get(objid)
        context.status_code = 404

    def head_cb(request, context):
        dirname, basename = request.path.rsplit("/", 1)
        objid = bytes.fromhex(basename)
        if dirname != "/content" or objid not in sto_back:
            context.status_code = 404
            return b"Not Found"
        return b"Found"

    mock.register_uri(requests_mock.GET, requests_mock.ANY, content=get_cb)
    mock.register_uri(requests_mock.HEAD, requests_mock.ANY, content=head_cb)

    return sto_front, sto_back, objids


def test_http_objstorage():
    sto_front, sto_back, objids = build_objstorage()

    for objid in objids:
        assert objid in sto_front
        assert sto_front.get(objid) == sto_back.get(objid)
        assert sto_front.get(objid).decode().startswith("some content ")


def test_http_objstorage_missing():
    sto_front, sto_back, objids = build_objstorage()

    assert b"\x00" * 20 not in sto_front


def test_http_objstorage_get_missing():
    sto_front, sto_back, objids = build_objstorage()

    with pytest.raises(exc.ObjNotFoundError):
        sto_front.get(b"\x00" * 20)


def test_http_objstorage_check():
    sto_front, sto_back, objids = build_objstorage()
    for objid in objids:
        assert sto_front.check(objid) is None  # no Exception means OK

    # create an invalid object in the in-memory objstorage
    invalid_content = b"p0wn3d content"
    fake_objid = "\x01" * 20
    sto_back.add(invalid_content, fake_objid)

    # the http objstorage should report it as invalid
    with pytest.raises(exc.Error):
        sto_front.check(fake_objid)


def test_http_objstorage_read_only():
    sto_front, sto_back, objids = build_objstorage()

    content = b""
    obj_id = compute_hash(content)
    with pytest.raises(exc.ReadOnlyObjStorage):
        sto_front.add(content, obj_id=obj_id)
    with pytest.raises(exc.ReadOnlyObjStorage):
        sto_front.restore(b"", obj_id=compute_hash(b""))
    with pytest.raises(exc.ReadOnlyObjStorage):
        sto_front.delete(b"\x00" * 20)


def test_http_objstorage_not_iterable():
    sto_front, sto_back, objids = build_objstorage()

    with pytest.raises(exc.NonIterableObjStorage):
        len(sto_front)
    with pytest.raises(exc.NonIterableObjStorage):
        iter(sto_front)


def test_http_cannonical_url():
    url = "http://127.0.0.1/content"
    sto = get_objstorage(cls="http", url=url)
    assert sto.root_path == url + "/"
