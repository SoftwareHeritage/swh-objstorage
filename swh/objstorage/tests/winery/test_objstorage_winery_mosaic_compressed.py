# Copyright (C) 2021-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import tempfile
import uuid

import pytest

from swh.mosaic import CompressionMethod, IdxDescription, MosaicCreator, MosaicReader
from swh.objstorage.objstorage import objid_for_content

from .winery_objstorage_testing import TestWinery as _TestWinery
from .winery_objstorage_testing import TestWineryObjStorage as _TestWineryObjStorage

logger = logging.getLogger(__name__)


@pytest.fixture
def pool_names():
    return ["winery-pool-active-mosaic-compressed"]


@pytest.fixture(scope="session")
def shards():
    count = 12
    nshards = 6
    shards = {}
    with tempfile.TemporaryDirectory() as shards_dir:
        for nshard in range(nshards):
            name = "i" + uuid.uuid4().hex[1:]
            path = os.path.join(shards_dir, name)
            shards[path] = []
            with MosaicCreator(
                path, [IdxDescription.SHA256FMPHGO], compression_level=3
            ) as mosaic:
                for i in range(count):
                    content = b"Housekeeping shard:%d content:%d" % (nshard, i)
                    objid = objid_for_content(content)
                    mosaic.add([objid["sha256"]], content)
                    shards[path].append(objid)
        yield shards


class TestMosaicCompressedWineryObjStorage(_TestWineryObjStorage):
    pass


class TestMosaicCompressedWinery(_TestWinery):
    def test_winery_standalone_packer(self, winery_settings, storage):
        super().test_winery_standalone_packer(winery_settings, storage)
        for name, _ in storage.writer.base.list_shards():
            shard = storage.reader.roshard(name)
            shard.open()
            assert isinstance(shard.shard, MosaicReader)
            assert shard.shard.compression == CompressionMethod.ZSTD
            shard.close()
