# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.objstorage.multiplexer.filter.filter import ObjStorageFilter


class ReadObjStorageFilter(ObjStorageFilter):
    """ Filter that disable write operation of the storage.

    Writes will always succeed without doing any actual write operations.
    """

    def check_config(self, *, check_write):
        return self.storage.check_config(check_write=False)

    def add(self, *args, **kwargs):
        return

    def restore(self, *args, **kwargs):
        return

    def delete(self, *args, **kwargs):
        return True
