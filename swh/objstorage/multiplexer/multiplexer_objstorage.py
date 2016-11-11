# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random

from ..objstorage import ObjStorage
from ..exc import ObjNotFoundError


class MultiplexerObjStorage(ObjStorage):
    """Implementation of ObjStorage that distributes between multiple
    storages.

    The multiplexer object storage allows an input to be demultiplexed
    among multiple storages that will or will not accept it by
    themselves (see .filter package).

    As the ids can be differents, no pre-computed ids should be
    submitted.  Also, there are no guarantees that the returned ids
    can be used directly into the storages that the multiplexer
    manage.


    Use case examples could be:
    Example 1:
        storage_v1 = filter.read_only(PathSlicingObjStorage('/dir1',
                                                            '0:2/2:4/4:6'))
        storage_v2 = PathSlicingObjStorage('/dir2', '0:1/0:5')
        storage = MultiplexerObjStorage([storage_v1, storage_v2])

        When using 'storage', all the new contents will only be added
        to the v2 storage, while it will be retrievable from both.

    Example 2:
        storage_v1 = filter.id_regex(
            PathSlicingObjStorage('/dir1', '0:2/2:4/4:6'),
            r'[^012].*'
        )
        storage_v2 = filter.if_regex(
            PathSlicingObjStorage('/dir2', '0:1/0:5'),
            r'[012]/*'
        )
        storage = MultiplexerObjStorage([storage_v1, storage_v2])

        When using this storage, the contents with a sha1 starting
        with 0, 1 or 2 will be redirected (read AND write) to the
        storage_v2, while the others will be redirected to the
        storage_v1.  If a content starting with 0, 1 or 2 is present
        in the storage_v1, it would be ignored anyway.

    """

    def __init__(self, storages):
        self.storages = storages

    def check_config(self, *, check_write):
        return all(
            storage.check_config(check_write=check_write)
            for storage in self.storages
        )

    def __contains__(self, obj_id):
        """Check the object storage for proper configuration.

        Args:
            check_write: check whether writes to the objstorage will succeed
        Returns:
            True if the storage is properly configured
        """
        for storage in self.storages:
            if obj_id in storage:
                return True
        return False

    def __iter__(self):
        """Iterates over the content of each storages

        Due to the demultiplexer nature, same content can be in multiple
        storages and may be yielded multiple times.

        Warning: The `__iter__` methods frequently have bad performance. You
        almost certainly don't want to use this method in production.

        """
        for storage in self.storages:
            yield from storage

    def __len__(self):
        """Compute the number of objects in the current object storage.

        Identical objects present in multiple storages will be counted as
        multiple objects.
        Warning: this currently uses `__iter__`, its warning about bad
        performance applies.

        Returns:
            number of objects contained in the storage.

        """
        return sum(map(len, self.storages))

    def add(self, content, obj_id=None, check_presence=True):
        """ Add a new object to the object storage.

        If the adding step works in all the storages that accept this content,
        this is a success. Otherwise, the full adding step is an error even if
        it succeed in some of the storages.

        Args:
            content: content of the object to be added to the storage.
            obj_id: checksum of [bytes] using [ID_HASH_ALGO] algorithm. When
                given, obj_id will be trusted to match the bytes. If missing,
                obj_id will be computed on the fly.
            check_presence: indicate if the presence of the content should be
                verified before adding the file.

        Returns:
            an id of the object into the storage. As the write-storages are
            always readable as well, any id will be valid to retrieve a
            content.
        """
        return [storage.add(content, obj_id, check_presence)
                for storage in self.storages].pop()

    def restore(self, content, obj_id=None):
        return [storage.restore(content, obj_id)
                for storage in self.storages].pop()

    def get(self, obj_id):
        for storage in self.storages:
            try:
                return storage.get(obj_id)
            except ObjNotFoundError:
                continue
        # If no storage contains this content, raise the error
        raise ObjNotFoundError(obj_id)

    def check(self, obj_id):
        nb_present = 0
        for storage in self.storages:
            try:
                storage.check(obj_id)
            except ObjNotFoundError:
                continue
            else:
                nb_present += 1
        # If there is an Error because of a corrupted file, then let it pass.

        # Raise the ObjNotFoundError only if the content coulnd't be found in
        # all the storages.
        if nb_present == 0:
            raise ObjNotFoundError(obj_id)

    def get_random(self, batch_size):
        storages_set = [storage for storage in self.storages
                        if len(storage) > 0]
        if len(storages_set) <= 0:
            return []

        while storages_set:
            storage = random.choice(storages_set)
            try:
                return storage.get_random(batch_size)
            except NotImplementedError:
                storages_set.remove(storage)
        # There is no storage that allow the get_random operation
        raise NotImplementedError(
            "There is no storage implementation into the multiplexer that "
            "support the 'get_random' operation"
        )
