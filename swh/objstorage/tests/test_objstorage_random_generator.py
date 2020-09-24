# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections.abc import Iterator

from swh.objstorage.factory import get_objstorage


def test_random_generator_objstorage():
    sto = get_objstorage("random", {})
    assert sto

    blobs = [sto.get(None) for i in range(100)]
    lengths = [len(x) for x in blobs]
    assert max(lengths) <= 55056238


def test_random_generator_objstorage_get_stream():
    sto = get_objstorage("random", {})
    gen = sto.get_stream(None)
    assert isinstance(gen, Iterator)
    assert list(gen)  # ensure the iterator can be consumed


def test_random_generator_objstorage_list_content():
    sto = get_objstorage("random", {"total": 100})
    assert isinstance(sto.list_content(), Iterator)

    assert list(sto.list_content()) == [b"%d" % i for i in range(1, 101)]
    assert list(sto.list_content(limit=10)) == [b"%d" % i for i in range(1, 11)]
    assert list(sto.list_content(last_obj_id=b"10", limit=10)) == [
        b"%d" % i for i in range(11, 21)
    ]


def test_random_generator_objstorage_total():
    sto = get_objstorage("random", {"total": 5})
    assert len([x for x in sto]) == 5


def test_random_generator_objstorage_size():
    sto = get_objstorage("random", {"filesize": 10})
    for i in range(10):
        assert len(sto.get(None)) == 10
