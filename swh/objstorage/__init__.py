# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from .objstorage import ObjStorage
from .objstorage_pathslicing import PathSlicingObjStorage
from .objstorage_in_memory import InMemoryObjStorage
from .api.client import RemoteObjStorage
from .multiplexer import MultiplexerObjStorage, StripingObjStorage
from .multiplexer.filter import add_filters


__all__ = ['get_objstorage', 'ObjStorage']


_STORAGE_CLASSES = {
    'pathslicing': PathSlicingObjStorage,
    'remote': RemoteObjStorage,
    'memory': InMemoryObjStorage,
}

_STORAGE_CLASSES_MISSING = {
}

try:
    from swh.objstorage.cloud.objstorage_azure import (
        AzureCloudObjStorage,
        PrefixedAzureCloudObjStorage,
    )
    _STORAGE_CLASSES['azure'] = AzureCloudObjStorage
    _STORAGE_CLASSES['azure-prefixed'] = PrefixedAzureCloudObjStorage
except ImportError as e:
    _STORAGE_CLASSES_MISSING['azure'] = e.args[0]
    _STORAGE_CLASSES_MISSING['azure-prefixed'] = e.args[0]

try:
    from swh.objstorage.objstorage_rados import RADOSObjStorage
    _STORAGE_CLASSES['rados'] = RADOSObjStorage
except ImportError as e:
    _STORAGE_CLASSES_MISSING['rados'] = e.args[0]


def get_objstorage(cls, args):
    """ Create an ObjStorage using the given implementation class.

    Args:
        cls (str): objstorage class unique key contained in the
            _STORAGE_CLASSES dict.
        args (dict): arguments for the required class of objstorage
            that must match exactly the one in the `__init__` method of the
            class.
    Returns:
        subclass of ObjStorage that match the given `storage_class` argument.
    Raises:
        ValueError: if the given storage class is not a valid objstorage
            key.
    """
    if cls in _STORAGE_CLASSES:
        return _STORAGE_CLASSES[cls](**args)
    else:
        raise ValueError('Storage class {} is not available: {}'.format(
                         cls,
                         _STORAGE_CLASSES_MISSING.get(cls, 'unknown name')))


def _construct_filtered_objstorage(storage_conf, filters_conf):
    return add_filters(
        get_objstorage(**storage_conf),
        filters_conf
    )


_STORAGE_CLASSES['filtered'] = _construct_filtered_objstorage


def _construct_multiplexer_objstorage(objstorages):
    storages = [get_objstorage(**conf)
                for conf in objstorages]
    return MultiplexerObjStorage(storages)


_STORAGE_CLASSES['multiplexer'] = _construct_multiplexer_objstorage


def _construct_striping_objstorage(objstorages):
    storages = [get_objstorage(**conf)
                for conf in objstorages]
    return StripingObjStorage(storages)


_STORAGE_CLASSES['striping'] = _construct_striping_objstorage
