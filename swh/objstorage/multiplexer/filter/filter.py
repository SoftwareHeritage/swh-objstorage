# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from ...objstorage import ObjStorage


class ObjStorageFilter(ObjStorage):
    """Base implementation of a filter that allow inputs on ObjStorage or
    not.

    This class copy the API of ...objstorage in order to filter the
    inputs of this class.

    If the operation is allowed, return the result of this operation
    applied to the destination implementation. Otherwise, just return
    without any operation.

    This class is an abstract base class for a classic read/write
    storage.  Filters can inherit from it and only redefine some
    methods in order to change behavior.

    """

    def __init__(self, storage):
        self.storage = storage

    def check_config(self, *, check_write):
        """Check the object storage for proper configuration.

        Args:
            check_write: check whether writes to the objstorage will succeed
        Returns:
            True if the storage is properly configured
        """
        return self.storage.check_config(check_write=check_write)

    def __contains__(self, *args, **kwargs):
        return self.storage.__contains__(*args, **kwargs)

    def __iter__(self):
        """ Iterates over the content of each storages

        Warning: The `__iter__` methods frequently have bad performance. You
        almost certainly don't want to use this method in production as the
        wrapped storage may cause performance issues.
        """
        return self.storage.__iter__()

    def __len__(self):
        """ Compute the number of objects in the current object storage.

        Warning: performance issue in `__iter__` also applies here.

        Returns:
            number of objects contained in the storage.
        """
        return self.storage.__len__()

    def add(self, content, obj_id=None, check_presence=True, *args, **kwargs):
        return self.storage.add(content, obj_id, check_presence,
                                *args, **kwargs)

    def restore(self, content, obj_id=None, *args, **kwargs):
        return self.storage.restore(content, obj_id, *args, **kwargs)

    def get(self, obj_id, *args, **kwargs):
        return self.storage.get(obj_id, *args, **kwargs)

    def check(self, obj_id, *args, **kwargs):
        return self.storage.check(obj_id, *args, **kwargs)

    def delete(self, obj_id, *args, **kwargs):
        return self.storage.delete(obj_id, *args, **kwargs)

    def get_random(self, batch_size, *args, **kwargs):
        return self.storage.get_random(batch_size, *args, **kwargs)
