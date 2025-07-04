# Copyright (C) 2021-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
import pytest

from swh.objstorage.backends.pathslicing import PathSlicer


def test_pathslicer():
    slicer = PathSlicer(root="/", slicing="0:2/2:4/4:6", primary_hash="sha1")
    assert len(slicer) == 3
    assert slicer.check_config() is None
    assert (
        slicer.get_path("34973274ccef6ab4dfaaf86599792fa9c3fe4689")
        == "/34/97/32/34973274ccef6ab4dfaaf86599792fa9c3fe4689"
    )
    assert (
        slicer.get_directory("34973274ccef6ab4dfaaf86599792fa9c3fe4689") == "/34/97/32"
    )
    assert slicer.get_slices("34973274ccef6ab4dfaaf86599792fa9c3fe4689") == [
        "34",
        "97",
        "32",
    ]

    slicer = PathSlicer(
        root="/", slicing="/0:1/0:5/", primary_hash="sha1"
    )  # trailing '/' are ignored
    assert slicer.check_config() is None
    assert len(slicer) == 2
    assert (
        slicer.get_path("34973274ccef6ab4dfaaf86599792fa9c3fe4689")
        == "/3/34973/34973274ccef6ab4dfaaf86599792fa9c3fe4689"
    )
    assert (
        slicer.get_directory("34973274ccef6ab4dfaaf86599792fa9c3fe4689") == "/3/34973"
    )
    assert slicer.get_slices("34973274ccef6ab4dfaaf86599792fa9c3fe4689") == [
        "3",
        "34973",
    ]

    # funny one, with steps
    slicer = PathSlicer(root="/", slicing="0:6:2/1:7:2", primary_hash="sha1")
    assert slicer.check_config() is None
    assert slicer.get_slices("123456789".ljust(40, "0")) == ["135", "246"]

    # reverse works too!
    slicer = PathSlicer(root="/", slicing="-1::-1", primary_hash="sha1")
    assert slicer.check_config() is None
    assert slicer.get_slices("34973274ccef6ab4dfaaf86599792fa9c3fe4689") == [
        "34973274ccef6ab4dfaaf86599792fa9c3fe4689"[::-1]
    ]


def test_pathslicer_noop():
    "test the 'empty' pathslicer"
    slicer = PathSlicer("/", "", "sha1")
    assert len(slicer) == 0
    assert slicer.check_config() is None
    assert (
        slicer.get_path("34973274ccef6ab4dfaaf86599792fa9c3fe4689")
        == "/34973274ccef6ab4dfaaf86599792fa9c3fe4689"
    )


def test_pathslicer_bad_hash():
    slicer = PathSlicer(root="/", slicing="0:2/2:4/4:6", primary_hash="sha1")
    for hexhash in ("0" * 39, "0" * 41, ""):
        with pytest.raises(AssertionError):
            slicer.get_path(hexhash)


def test_pathslicer_check_config():
    with pytest.raises(ValueError):
        PathSlicer(root="/", slicing="toto", primary_hash="sha1")

    with pytest.raises(ValueError):
        PathSlicer(root="/", slicing="/1:2/a:b/", primary_hash="sha1")

    assert (
        PathSlicer(root="/", slicing="0:40", primary_hash="sha1").check_config() is None
    )
    with pytest.raises(ValueError):
        PathSlicer(root="/", slicing="0:41", primary_hash="sha1").check_config()
    assert (
        PathSlicer(root="/", slicing="40:", primary_hash="sha1").check_config() is None
    )
    with pytest.raises(ValueError):
        PathSlicer(root="/", slicing="41:", primary_hash="sha1").check_config()
