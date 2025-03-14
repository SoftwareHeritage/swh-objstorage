# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Literal, NotRequired, Optional, Tuple, TypedDict

# This would be used for image features that are not supported by the kernel RBD
# driver, e.g. exclusive-lock, object-map and fast-diff for kernels < 5.3
DEFAULT_IMAGE_FEATURES_UNSUPPORTED: Tuple[str, ...] = ()


class Packer(TypedDict):
    """Settings for the packer process, either external or internal"""

    create_images: NotRequired[bool]
    """Whether to create the images"""
    pack_immediately: NotRequired[bool]
    """Immediately pack shards (in a separate thread) when overflowing"""
    clean_immediately: NotRequired[bool]
    """Immediately clean shards when packing is complete"""


def packer_settings_with_defaults(values: Packer) -> Packer:
    """Hydrate Packer settings with default values"""
    return {
        "create_images": True,
        "pack_immediately": True,
        "clean_immediately": True,
        **values,
    }


class Shards(TypedDict):
    """Settings for shard management"""

    max_size: int
    """Maximum cumulative size of objects in a shard"""
    rw_idle_timeout: NotRequired[float]
    """Timeout (seconds) after which write shards get released when idle"""


def shards_settings_with_defaults(values: Shards) -> Shards:
    """Hydrate Shards settings with default values"""
    return {"rw_idle_timeout": 300, **values}


class ShardsPool(TypedDict):
    """Settings for the Shards pool"""

    type: Literal["rbd", "directory"]


class RbdShardsPool(ShardsPool, TypedDict):
    """Settings for the Ceph RBD-based Shards pool"""

    use_sudo: NotRequired[bool]
    map_options: NotRequired[str]
    pool_name: NotRequired[str]
    data_pool_name: NotRequired[Optional[str]]
    image_features_unsupported: NotRequired[Tuple[str, ...]]


def rbd_shards_pool_settings_with_defaults(
    values: ShardsPool,
) -> RbdShardsPool:
    """Hydrate RbdShards settings with default values"""
    return {
        "type": "rbd",
        "use_sudo": True,
        "pool_name": "shards",
        "data_pool_name": None,
        "image_features_unsupported": DEFAULT_IMAGE_FEATURES_UNSUPPORTED,
        "map_options": "",
        **values,
    }


class DirectoryShardsPool(ShardsPool, TypedDict):
    """Settings for the File-based Shards pool"""

    base_directory: str
    pool_name: NotRequired[str]


def directory_shards_pool_settings_with_defaults(
    values: ShardsPool,
) -> DirectoryShardsPool:
    """Hydrate RbdShards settings with default values"""
    if values["type"] != "directory":
        raise ValueError(
            f"Instantiating a directory shards pool with the wrong type: {values['type']}"
        )
    if "base_directory" not in values:
        raise ValueError(
            "Missing base_directory setting for Directory-based shards pool"
        )
    return {
        "type": "directory",
        "pool_name": values.get("pool_name", "shards"),  # type: ignore[typeddict-item]
        "base_directory": values["base_directory"],  # type: ignore[typeddict-item]
    }


class Throttler(TypedDict):
    """Settings for the winery throttler"""

    db: NotRequired[str]
    """Throttler database connection string"""
    max_read_bps: int
    """Max read bytes per second"""
    max_write_bps: int
    """Max write bytes per second"""


class Database(TypedDict):
    """Settings for the winery database"""

    db: str
    """Database connection string"""
    application_name: NotRequired[Optional[str]]
    """Application name for the database connection"""


def database_settings_with_defaults(values: Database) -> Database:
    """Hydrate Database settings with defaults"""
    return {"application_name": None, **values}


class Winery(TypedDict, total=False):
    """A representation of all available winery settings"""

    database: Database
    shards: Shards
    shards_pool: ShardsPool
    throttler: Optional[Throttler]
    packer: Packer


SETTINGS = frozenset({"database", "shards", "shards_pool", "throttler", "packer"})


def populate_default_settings(
    database: Optional[Database] = None,
    shards: Optional[Shards] = None,
    shards_pool: Optional[ShardsPool] = None,
    throttler: Optional[Throttler] = None,
    packer: Optional[Packer] = None,
) -> Winery:
    """Given some settings for a Winery objstorage, add all the appropriate
    default settings."""
    settings: Winery = {}

    if database is not None:
        database = database_settings_with_defaults(database)
        settings["database"] = database

    if shards is not None:
        shards = shards_settings_with_defaults(shards)
        settings["shards"] = shards

    if shards_pool is not None:
        if shards_pool["type"] == "rbd":
            shards_pool = rbd_shards_pool_settings_with_defaults(shards_pool)
            settings["shards_pool"] = shards_pool
        elif shards_pool["type"] == "directory":
            shards_pool = directory_shards_pool_settings_with_defaults(shards_pool)
            settings["shards_pool"] = shards_pool
        else:
            raise ValueError(f"Unknown shards pool type: {shards_pool['type']}")

    if throttler is not None:
        if "db" not in throttler:
            settings["throttler"] = {"db": settings["database"]["db"], **throttler}
        else:
            settings["throttler"] = throttler

    if packer is not None:
        packer = packer_settings_with_defaults(packer)
        settings["packer"] = packer

    return settings
