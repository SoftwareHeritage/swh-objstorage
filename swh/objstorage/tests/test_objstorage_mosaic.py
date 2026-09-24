# Copyright (C) 2025-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.mosaic import IdxDescription, MosaicCreator
from swh.objstorage.factory import get_objstorage
from swh.objstorage.objstorage import ObjNotFoundError, objid_for_content


# @pytest.fixture(params=("sha1", "sha1_git", "sha256"))
@pytest.fixture
def mosaic_file(request, tmpdir):
    """Fills a temporary shard file with 100 objects."""

    # primary_hash = request.param
    primary_hash = "sha1_git"
    mosaic_path = tmpdir / f"test_{primary_hash}.mosaic"
    nb_objects = 100
    contents = [f"some content {i}".encode() for i in range(nb_objects)]
    obj_ids = []
    with MosaicCreator(str(mosaic_path), [IdxDescription.SHA1GITFMPHGO]) as mosaic:
        for content in contents:
            obj_id = objid_for_content(content)
            obj_ids.append(obj_id)
            key = obj_id[primary_hash]
            mosaic.add([key], content)
    return mosaic_path, primary_hash, obj_ids


def test_mosaic_objstorage(request, mosaic_file):
    mosaic_path, primary_hash, obj_ids = mosaic_file
    objstorage = get_objstorage(
        cls="mosaic", path=str(mosaic_path), primary_hash=primary_hash, compression=None
    )
    objects = [objstorage.get(obj_id) for obj_id in obj_ids]
    assert sorted(objects) == sorted([f"some content {i}".encode() for i in range(100)])
    for obj_id in obj_ids:
        objstorage.check(obj_id)
        assert obj_id in objstorage
    fake_key = objid_for_content(b"404 should not exist")
    with pytest.raises(ObjNotFoundError):
        objstorage.check(fake_key)
    with pytest.raises(ObjNotFoundError):
        _ = objstorage.get(fake_key)
