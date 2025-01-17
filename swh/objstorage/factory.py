# Copyright (C) 2016-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.core.config import get_swh_backend_module
from swh.objstorage.interface import ObjStorageInterface
from swh.objstorage.objstorage import ObjStorage

__all__ = ["get_objstorage", "ObjStorage"]


def get_objstorage(cls: str, **kwargs) -> ObjStorageInterface:
    """Create an ObjStorage using the given implementation class.

    Args:
        cls: objstorage class unique key declared in the
            swh.objstorage.classes entry point.
        kwargs: arguments for the required class of objstorage
                that must match exactly the one in the `__init__` method of the
                class.
    Returns:
        subclass of ObjStorage that match the given `storage_class` argument.
    Raises:
        ValueError: if the given storage class is not a valid objstorage
            key.
    """

    _, ObjStorage = get_swh_backend_module("objstorage", cls)
    assert ObjStorage is not None
    check_config = kwargs.pop("check_config", {})
    objstorage = ObjStorage(**kwargs)
    if check_config:
        if not objstorage.check_config(**check_config):
            raise EnvironmentError("objstorage check config failed")
    return objstorage
