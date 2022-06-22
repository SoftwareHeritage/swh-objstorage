# Copyright (C) 2016-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import importlib
import warnings

from swh.objstorage.multiplexer import MultiplexerObjStorage, StripingObjStorage
from swh.objstorage.multiplexer.filter import add_filters
from swh.objstorage.objstorage import ObjStorage

__all__ = ["get_objstorage", "ObjStorage"]


OBJSTORAGE_IMPLEMENTATIONS = {
    "pathslicing": ".backends.pathslicing.PathSlicingObjStorage",
    "remote": ".api.client.RemoteObjStorage",
    "memory": ".backends.in_memory.InMemoryObjStorage",
    "seaweedfs": ".backends.seaweedfs.SeaweedFilerObjStorage",
    "random": ".backends.generator.RandomGeneratorObjStorage",
    "http": ".backends.http.HTTPReadOnlyObjStorage",
    "noop": ".backends.noop.NoopObjStorage",
    "azure": ".backends.azure.AzureCloudObjStorage",
    "azure-prefixed": ".backends.azure.PrefixedAzureCloudObjStorage",
    "s3": ".backends.libcloud.AwsCloudObjStorage",
    "swift": ".backends.libcloud.OpenStackCloudObjStorage",
    "winery": ".backends.winery.WineryObjStorage",
}


def get_objstorage(cls: str, args=None, **kwargs):
    """Create an ObjStorage using the given implementation class.

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
    if args is not None:
        warnings.warn(
            'Explicit "args" key is deprecated for objstorage initialization, '
            "use class arguments keys directly instead.",
            DeprecationWarning,
        )
        # TODO: when removing this, drop the "args" backwards compatibility
        # from swh.objstorage.api.server configuration checker
        kwargs = args

    class_path = OBJSTORAGE_IMPLEMENTATIONS.get(cls)
    if class_path is None:
        raise ValueError(
            "Unknown storage class `%s`. Supported: %s"
            % (cls, ", ".join(OBJSTORAGE_IMPLEMENTATIONS))
        )

    if "." in class_path:
        (module_path, class_name) = class_path.rsplit(".", 1)
        try:
            module = importlib.import_module(module_path, package=__package__)
        except ImportError as e:
            raise ValueError(f"Storage class {cls} is not available: {e.args[0]}")
        ObjStorage = getattr(module, class_name)
    else:
        ObjStorage = globals()[class_path]

    return ObjStorage(**kwargs)


def _construct_filtered_objstorage(storage_conf, filters_conf):
    return add_filters(get_objstorage(**storage_conf), filters_conf)


OBJSTORAGE_IMPLEMENTATIONS["filtered"] = "_construct_filtered_objstorage"


def _construct_multiplexer_objstorage(objstorages):
    storages = [get_objstorage(**conf) for conf in objstorages]
    return MultiplexerObjStorage(storages)


OBJSTORAGE_IMPLEMENTATIONS["multiplexer"] = "_construct_multiplexer_objstorage"


def _construct_striping_objstorage(objstorages):
    storages = [get_objstorage(**conf) for conf in objstorages]
    return StripingObjStorage(storages)


OBJSTORAGE_IMPLEMENTATIONS["striping"] = "_construct_striping_objstorage"
