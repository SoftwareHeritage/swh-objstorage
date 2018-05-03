# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from .objstorage import ObjStorage
from .objstorage_pathslicing import PathSlicingObjStorage
from .objstorage_in_memory import InMemoryObjStorage
from .api.client import RemoteObjStorage
from .multiplexer import MultiplexerObjStorage
from .multiplexer.filter import add_filters


__all__ = ['get_objstorage', 'ObjStorage']


_STORAGE_CLASSES = {
    'pathslicing': PathSlicingObjStorage,
    'remote': RemoteObjStorage,
    'in-memory': InMemoryObjStorage,
}

try:
    from swh.objstorage.cloud.objstorage_azure import (
        AzureCloudObjStorage,
        PrefixedAzureCloudObjStorage,
    )
    _STORAGE_CLASSES['azure'] = AzureCloudObjStorage
    _STORAGE_CLASSES['azure-prefixed'] = PrefixedAzureCloudObjStorage
except ImportError:
    pass

try:
    from swh.objstorage.objstorage_rados import RADOSObjStorage
    _STORAGE_CLASSES['rados'] = RADOSObjStorage
except ImportError:
    pass


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
    try:
        return _STORAGE_CLASSES[cls](**args)
    except KeyError:
        raise ValueError('Storage class %s does not exist' % cls)


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
