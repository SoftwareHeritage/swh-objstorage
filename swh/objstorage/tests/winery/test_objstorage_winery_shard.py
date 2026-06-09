# Copyright (C) 2021-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

import pytest

from .winery_objstorage_testing import TestWinery as _TestWinery
from .winery_objstorage_testing import TestWineryObjStorage as _TestWineryObjStorage

logger = logging.getLogger(__name__)


@pytest.fixture
def pool_names():
    return ["winery-pool-active-directory"]


class TestShardDirectoryWineryObjStorage(_TestWineryObjStorage):
    pass


class TestShardDirectoryWinery(_TestWinery):
    pass
