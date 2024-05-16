# Copyright (C) 2016-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import importlib
import warnings

from swh.objstorage.interface import ObjStorageInterface
from swh.objstorage.objstorage import ObjStorage

__all__ = ["get_objstorage", "ObjStorage"]


OBJSTORAGE_IMPLEMENTATIONS = {
    "pathslicing": "swh.objstorage.backends.pathslicing.PathSlicingObjStorage",
    "remote": "swh.objstorage.api.client.RemoteObjStorage",
    "memory": "swh.objstorage.backends.in_memory.InMemoryObjStorage",
    "seaweedfs": "swh.objstorage.backends.seaweedfs.objstorage.SeaweedFilerObjStorage",
    "random": "swh.objstorage.backends.generator.RandomGeneratorObjStorage",
    "http": "swh.objstorage.backends.http.HTTPReadOnlyObjStorage",
    "noop": "swh.objstorage.backends.noop.NoopObjStorage",
    "azure": "swh.objstorage.backends.azure.AzureCloudObjStorage",
    "azure-prefixed": "swh.objstorage.backends.azure.PrefixedAzureCloudObjStorage",
    "s3": "swh.objstorage.backends.libcloud.AwsCloudObjStorage",
    "swift": "swh.objstorage.backends.libcloud.OpenStackCloudObjStorage",
    "winery": "swh.objstorage.backends.winery.WineryObjStorage",
    # filters and proxies
    "multiplexer": "swh.objstorage.multiplexer.MultiplexerObjStorage",
    "read-only": "swh.objstorage.proxies.readonly.ReadOnlyProxyObjStorage",
    # deprecated factories
    "filtered": "_construct_filtered_objstorage",
}


def get_objstorage(cls: str, **kwargs) -> ObjStorageInterface:
    """Create an ObjStorage using the given implementation class.

    Args:
        cls: objstorage class unique key contained in the
            OBJSTORAGE_IMPLEMENTATIONS dict.
        kwargs: arguments for the required class of objstorage
                that must match exactly the one in the `__init__` method of the
                class.
    Returns:
        subclass of ObjStorage that match the given `storage_class` argument.
    Raises:
        ValueError: if the given storage class is not a valid objstorage
            key.
    """
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
        # used by (deprecated) filtered for which the value is a factory
        # function rather than a class
        ObjStorage = globals()[class_path]
    return ObjStorage(**kwargs)


def _construct_filtered_objstorage(storage_conf, filters_conf, **kwargs):
    if len(filters_conf) != 1 or filters_conf[0]["type"] != "readonly":
        raise ValueError("This legacy function only supports a single readonly filter")
    warnings.warn(
        "The 'filtered[type:readonly]' objstorage class has been deprecated, "
        "please use a 'read-only' proxy class instead.",
        DeprecationWarning,
    )

    return get_objstorage(cls="read-only", storage=get_objstorage(**storage_conf))
