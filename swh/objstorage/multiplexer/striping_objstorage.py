# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from .multiplexer_objstorage import MultiplexerObjStorage


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
            raise ValueError(
                'StripingObjStorage always needs obj_id to be set'
            )

        index = int.from_bytes(obj_id[:-self.MOD_BYTES], 'little')
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
