# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.objstorage.objstorage import ObjStorage


class NoopObjStorage(ObjStorage):
    """Noop objstorage. Basic implementation which does no operations at all.

    Only intended for test purposes to avoid either memory or i/o operations. This
    allows swh clients to use the swh stack without having to deal with objstorage
    configuration. So users can concentrate on testing the remaining part of the stack
    without the objstorage.

    """

    def check_config(self, *, check_write):
        return True

    def __contains__(self, obj_id, *args, **kwargs):
        return False

    def add(self, content, obj_id, check_presence=True, *args, **kwargs):
        pass

    def get(self, obj_id, *args, **kwargs):
        return None

    def check(self, obj_id, *args, **kwargs):
        pass

    def delete(self, obj_id, *args, **kwargs):
        pass
