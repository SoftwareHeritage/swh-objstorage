# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import queue

from swh.objstorage.multiplexer.multiplexer_objstorage import (
    MultiplexerObjStorage,
    ObjStorageThread,
)


class StripingObjStorage(MultiplexerObjStorage):
    """Stripes objects across multiple objstorages

    This objstorage implementation will write objects to objstorages in a
    predictable way: it takes the modulo of the last 8 bytes of the object
    identifier with the number of object storages passed, which will yield an
    (almost) even distribution.

    Objects are read from all storages in turn until it succeeds.

    """

    MOD_BYTES = 8

    def __init__(self, storages, **kwargs):
        super().__init__(storages, **kwargs)
        self.num_storages = len(storages)

    def get_storage_index(self, obj_id):
        if obj_id is None:
            raise ValueError("StripingObjStorage always needs obj_id to be set")

        index = int.from_bytes(obj_id[: -self.MOD_BYTES], "big")
        return index % self.num_storages

    def get_write_threads(self, obj_id):
        idx = self.get_storage_index(obj_id)
        yield self.storage_threads[idx]

    def get_read_threads(self, obj_id=None):
        if obj_id:
            idx = self.get_storage_index(obj_id)
        else:
            idx = 0
        for i in range(self.num_storages):
            yield self.storage_threads[(idx + i) % self.num_storages]

    def add_batch(self, contents, check_presence=True):
        """Add a batch of new objects to the object storage.

        """
        content_by_storage_index = defaultdict(dict)
        for obj_id, content in contents.items():
            storage_index = self.get_storage_index(obj_id)
            content_by_storage_index[storage_index][obj_id] = content

        mailbox = queue.Queue()
        for storage_index, contents in content_by_storage_index.items():
            self.storage_threads[storage_index].queue_command(
                "add_batch", contents, check_presence=check_presence, mailbox=mailbox,
            )

        results = ObjStorageThread.collect_results(
            mailbox, len(content_by_storage_index)
        )
        summed = {"object:add": 0, "object:add:bytes": 0}
        for result in results:
            summed["object:add"] += result["object:add"]
            summed["object:add:bytes"] += result["object:add:bytes"]
        return summed
