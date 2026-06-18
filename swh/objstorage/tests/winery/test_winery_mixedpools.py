# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

import pytest

from .winery_objstorage_testing import TestWinery as _TestWinery
from .winery_objstorage_testing import TestWineryObjStorage as _TestWineryObjStorage

logger = logging.getLogger(__name__)


@pytest.fixture
def pool_names(request, pytestconfig):
    return [
        "winery-pool-01-rbd",
        "winery-pool-02-active-directory",
    ]


class TestWineryMixedpoolObjStorage(_TestWineryObjStorage):

    pass


class TestMixedpoolWinery(_TestWinery):
    """
    This tests requires Ceph and USE_CEPH=yes ; it checks the "mixed pools" case, where
    we read from a Ceph storage and write to directories.

    Reading objects from all pools is indirectly tested by `test_winery_reader_lru`.
    """

    pass
