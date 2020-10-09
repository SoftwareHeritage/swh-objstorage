# Copyright (C) 2016-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Callable, Dict, Union
import warnings

from swh.objstorage.api.client import RemoteObjStorage
from swh.objstorage.backends.generator import RandomGeneratorObjStorage
from swh.objstorage.backends.in_memory import InMemoryObjStorage
from swh.objstorage.backends.pathslicing import PathSlicingObjStorage
from swh.objstorage.backends.seaweed import WeedObjStorage
from swh.objstorage.multiplexer import MultiplexerObjStorage, StripingObjStorage
from swh.objstorage.multiplexer.filter import add_filters
from swh.objstorage.objstorage import ID_HASH_LENGTH, ObjStorage  # noqa

__all__ = ["get_objstorage", "ObjStorage"]


_STORAGE_CLASSES: Dict[str, Union[type, Callable[..., type]]] = {
    "pathslicing": PathSlicingObjStorage,
    "remote": RemoteObjStorage,
    "memory": InMemoryObjStorage,
    "weed": WeedObjStorage,
    "random": RandomGeneratorObjStorage,
}

_STORAGE_CLASSES_MISSING = {}

try:
    from swh.objstorage.backends.azure import (
        AzureCloudObjStorage,
        PrefixedAzureCloudObjStorage,
    )

    _STORAGE_CLASSES["azure"] = AzureCloudObjStorage
    _STORAGE_CLASSES["azure-prefixed"] = PrefixedAzureCloudObjStorage
except ImportError as e:
    _STORAGE_CLASSES_MISSING["azure"] = e.args[0]
    _STORAGE_CLASSES_MISSING["azure-prefixed"] = e.args[0]

try:
    from swh.objstorage.backends.rados import RADOSObjStorage

    _STORAGE_CLASSES["rados"] = RADOSObjStorage
except ImportError as e:
    _STORAGE_CLASSES_MISSING["rados"] = e.args[0]

try:
    from swh.objstorage.backends.libcloud import (
        AwsCloudObjStorage,
        OpenStackCloudObjStorage,
    )

    _STORAGE_CLASSES["s3"] = AwsCloudObjStorage
    _STORAGE_CLASSES["swift"] = OpenStackCloudObjStorage
except ImportError as e:
    _STORAGE_CLASSES_MISSING["s3"] = e.args[0]
    _STORAGE_CLASSES_MISSING["swift"] = e.args[0]


def get_objstorage(cls: str, args=None, **kwargs):
    """ Create an ObjStorage using the given implementation class.

    Args:
        cls: objstorage class unique key contained in the
            _STORAGE_CLASSES dict.
        kwargs: arguments for the required class of objstorage
                that must match exactly the one in the `__init__` method of the
                class.
    Returns:
        subclass of ObjStorage that match the given `storage_class` argument.
    Raises:
        ValueError: if the given storage class is not a valid objstorage
            key.
    """
    if cls in _STORAGE_CLASSES:
        if args is not None:
            warnings.warn(
                'Explicit "args" key is deprecated for objstorage initialization, '
                "use class arguments keys directly instead.",
                DeprecationWarning,
            )
            # TODO: when removing this, drop the "args" backwards compatibility
            # from swh.objstorage.api.server configuration checker
            kwargs = args

        return _STORAGE_CLASSES[cls](**kwargs)
    else:
        raise ValueError(
            "Storage class {} is not available: {}".format(
                cls, _STORAGE_CLASSES_MISSING.get(cls, "unknown name")
            )
        )


def _construct_filtered_objstorage(storage_conf, filters_conf):
    return add_filters(get_objstorage(**storage_conf), filters_conf)


_STORAGE_CLASSES["filtered"] = _construct_filtered_objstorage


def _construct_multiplexer_objstorage(objstorages):
    storages = [get_objstorage(**conf) for conf in objstorages]
    return MultiplexerObjStorage(storages)


_STORAGE_CLASSES["multiplexer"] = _construct_multiplexer_objstorage


def _construct_striping_objstorage(objstorages):
    storages = [get_objstorage(**conf) for conf in objstorages]
    return StripingObjStorage(storages)


_STORAGE_CLASSES["striping"] = _construct_striping_objstorage
