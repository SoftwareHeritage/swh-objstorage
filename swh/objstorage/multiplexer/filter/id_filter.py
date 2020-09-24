# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import re

from swh.model import hashutil
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.multiplexer.filter.filter import ObjStorageFilter
from swh.objstorage.objstorage import compute_hash


class IdObjStorageFilter(ObjStorageFilter, metaclass=abc.ABCMeta):
    """ Filter that only allow operations if the object id match a requirement.

    Even for read operations, check before if the id match the requirements.
    This may prevent for unnecessary disk access.
    """

    @abc.abstractmethod
    def is_valid(self, obj_id):
        """ Indicates if the given id is valid.
        """
        raise NotImplementedError(
            "Implementations of an IdObjStorageFilter " 'must have a "is_valid" method'
        )

    def __contains__(self, obj_id, *args, **kwargs):
        if self.is_valid(obj_id):
            return self.storage.__contains__(*args, obj_id=obj_id, **kwargs)
        return False

    def __len__(self):
        return sum(1 for i in [id for id in self.storage if self.is_valid(id)])

    def __iter__(self):
        yield from filter(lambda id: self.is_valid(id), iter(self.storage))

    def add(self, content, obj_id=None, check_presence=True, *args, **kwargs):
        if obj_id is None:
            obj_id = compute_hash(content)
        if self.is_valid(obj_id):
            return self.storage.add(content, *args, obj_id=obj_id, **kwargs)

    def restore(self, content, obj_id=None, *args, **kwargs):
        if obj_id is None:
            obj_id = compute_hash(content)
        if self.is_valid(obj_id):
            return self.storage.restore(content, *args, obj_id=obj_id, **kwargs)

    def get(self, obj_id, *args, **kwargs):
        if self.is_valid(obj_id):
            return self.storage.get(*args, obj_id=obj_id, **kwargs)
        raise ObjNotFoundError(obj_id)

    def check(self, obj_id, *args, **kwargs):
        if self.is_valid(obj_id):
            return self.storage.check(*args, obj_id=obj_id, **kwargs)
        raise ObjNotFoundError(obj_id)

    def get_random(self, *args, **kwargs):
        yield from filter(
            lambda id: self.is_valid(id), self.storage.get_random(*args, **kwargs)
        )


class RegexIdObjStorageFilter(IdObjStorageFilter):
    """ Filter that allow operations if the content's id as hex match a regex.
    """

    def __init__(self, storage, regex):
        super().__init__(storage)
        self.regex = re.compile(regex)

    def is_valid(self, obj_id):
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        return self.regex.match(hex_obj_id) is not None


class PrefixIdObjStorageFilter(IdObjStorageFilter):
    """ Filter that allow operations if the hexlified id have a given prefix.
    """

    def __init__(self, storage, prefix):
        super().__init__(storage)
        self.prefix = str(prefix)

    def is_valid(self, obj_id):
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        return str(hex_obj_id).startswith(self.prefix)
