# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from .filter import ObjStorageFilter


class ResilientStorageFilter(ObjStorageFilter):
    """ Filter that disable delete operation of the storage.

    Deletes will always succeed without doing any actual write operations.
    """
    def delete(self, *args, **kwargs):
        return
