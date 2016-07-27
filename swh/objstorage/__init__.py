from .objstorage import ObjStorage
from .objstorage_pathslicing import PathSlicingObjStorage
from .api.client import RemoteObjStorage
from .multiplexer import MultiplexerObjStorage
from .multiplexer.filter import add_filters

# TODO remove PathSlicingObjStorage from this list once the config
# loading will be updated and no hardcoded objstorage types should
# remains.
__all__ = ['get_objstorage', 'ObjStorage', 'PathSlicingObjStorage']

_STORAGE_CLASSES = {
    'pathslicing': PathSlicingObjStorage,
    'remote': RemoteObjStorage,
}


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
        raise ValueError('Storage class %s does not exists' % cls)


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
