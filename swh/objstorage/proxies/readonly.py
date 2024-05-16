# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, Iterator, Union

from swh.objstorage.exc import ReadOnlyObjStorageError
from swh.objstorage.factory import get_objstorage
from swh.objstorage.interface import CompositeObjId, ObjStorageInterface
from swh.objstorage.objstorage import ObjStorage


class ReadOnlyProxyObjStorage(ObjStorage):
    """Filter that disable write operation of the storage.

    Writes will always succeed without doing any actual write operations.
    """

    name: str = "read-only"

    def __init__(self, storage: Union[ObjStorageInterface, Dict], **kwargs):
        super().__init__(**kwargs)
        self.storage: ObjStorageInterface = (
            get_objstorage(**storage) if isinstance(storage, dict) else storage
        )

    def __contains__(self, *args, **kwargs):
        return self.storage.__contains__(*args, **kwargs)

    def __iter__(self) -> Iterator[CompositeObjId]:
        """Iterates over the content of each storages

        Warning: The `__iter__` methods frequently have bad performance. You
        almost certainly don't want to use this method in production as the
        wrapped storage may cause performance issues.
        """
        return self.storage.__iter__()

    def __len__(self):
        """Compute the number of objects in the current object storage.

        Warning: performance issue in `__iter__` also applies here.

        Returns:
            number of objects contained in the storage.
        """
        return self.storage.__len__()

    def get(self, obj_id, *args, **kwargs):
        return self.storage.get(obj_id, *args, **kwargs)

    def check(self, obj_id, *args, **kwargs):
        return self.storage.check(obj_id, *args, **kwargs)

    def check_config(self, *, check_write):
        if check_write:
            return False
        return self.storage.check_config(check_write=False)

    def add(self, *args, **kwargs):
        raise ReadOnlyObjStorageError("add")

    def restore(self, *args, **kwargs):
        raise ReadOnlyObjStorageError("restore")

    def delete(self, *args, **kwargs):
        raise ReadOnlyObjStorageError("dalete")
