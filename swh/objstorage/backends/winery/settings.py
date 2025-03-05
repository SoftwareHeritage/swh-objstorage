# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Literal, NotRequired, Optional, Tuple, TypedDict

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

    type: Literal["rbd"]


class RbdShardsPool(TypedDict):
    """Settings for the Ceph RBD-based Shards pool"""

    type: Literal["rbd"]
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
    shards_pool: RbdShardsPool
    throttler: Optional[Throttler]
    packer: Packer


SETTINGS = frozenset({"database", "shards", "shards_pool", "throttler", "packer"})


def populate_default_settings(
    database: Optional[Database] = None,
    shards: Optional[Shards] = None,
    shards_pool: Optional[ShardsPool] = None,
    throttler: Optional[Throttler] = None,
    packer: Optional[Packer] = None,
) -> Tuple[Winery, Dict[str, Any]]:
    """Given some settings for a Winery objstorage, add all the appropriate
    default settings."""
    settings: Winery = {}
    legacy_kwargs: Dict[str, Any] = {}

    if database is not None:
        database = database_settings_with_defaults(database)
        settings["database"] = database
        legacy_kwargs["base_dsn"] = database["db"]
        legacy_kwargs["application_name"] = database["application_name"]

    if shards is not None:
        shards = shards_settings_with_defaults(shards)
        settings["shards"] = shards
        legacy_kwargs["rwshard_idle_timeout"] = shards["rw_idle_timeout"]

    if shards_pool is not None:
        if shards_pool["type"] == "rbd":
            shards_pool = rbd_shards_pool_settings_with_defaults(shards_pool)
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

    return settings, legacy_kwargs
