# Copyright (C) 2025-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from typing import Any, Iterable, List, Literal, NotRequired, Optional, Tuple, TypedDict

logger = logging.getLogger(__name__)

# This would be used for image features that are not supported by the kernel RBD
# driver, e.g. exclusive-lock, object-map and fast-diff for kernels < 5.3
DEFAULT_IMAGE_FEATURES_UNSUPPORTED: Tuple[str, ...] = ()

SHARD_CACHE_DEFAULT_SIZE = 1000


class Packer(TypedDict):
    """Settings for the packer process, either external or internal"""

    create_images: NotRequired[bool]
    """Whether to create the images"""
    pack_immediately: NotRequired[bool]
    """Deprecated (always False)"""
    clean_immediately: NotRequired[bool]
    """Deprecated (always False)"""


def packer_settings_with_defaults(values: Packer) -> Packer:
    """Hydrate Packer settings with default values"""
    return {
        "create_images": True,
        "pack_immediately": False,
        "clean_immediately": False,
        **values,
    }


class ShardsPool(TypedDict):
    """Settings for the Shards pool"""

    type: Literal["rbd", "directory", "mosaic"]
    pool_name: NotRequired[str]
    shard_max_size: int


class RbdShardsPool(ShardsPool, TypedDict):
    """Settings for the Ceph RBD-based Shards pool"""

    use_sudo: NotRequired[bool]
    map_options: NotRequired[str]
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
    use_permissions: bool


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
        "pool_name": values.get("pool_name", "shards"),
        "shard_max_size": values["shard_max_size"],
        "base_directory": values["base_directory"],  # type: ignore[typeddict-item]
        "use_permissions": values.get("use_permissions", True),  # type: ignore[typeddict-item]
    }


class MosaicShardsPool(ShardsPool, TypedDict):
    """Settings for the MOSAIC-based Shards pool"""

    base_directory: str
    compression_level: Optional[int]


def mosaic_pool_settings_with_defaults(
    values: ShardsPool,
) -> MosaicShardsPool:
    """Hydrate MOSAIC settings with default values"""
    if values["type"] != "mosaic":
        raise ValueError(
            f"Instantiating a mosaic pool with the wrong type: {values['type']}"
        )
    if "base_directory" not in values:
        raise ValueError("Missing base_directory setting for MOSAIC-based pool")
    provided_level = values.get("compression_level", "none")
    if provided_level is None or provided_level == "none":
        compression_level = None
    else:
        compression_level = int(provided_level)  # type: ignore[call-overload]
    return {
        "type": "mosaic",
        "pool_name": values.get("pool_name", "mosaics"),
        "shard_max_size": values["shard_max_size"],
        "base_directory": values["base_directory"],  # type: ignore[typeddict-item]
        "compression_level": compression_level,
    }


class MosaicS3ShardsPool(ShardsPool, TypedDict):
    """Settings for the MOSAIC-S3-based Shards pool"""

    base_url: str
    compression_level: Optional[int]
    anonymous: bool
    image_extension: str | None


def mosaic_s3_pool_settings_with_defaults(
    values: ShardsPool,
) -> MosaicS3ShardsPool:
    """Hydrate MOSAIC settings with default values"""
    if values["type"] != "mosaic-s3":
        raise ValueError(
            f"Instantiating a mosaic pool with the wrong type: {values['type']}"
        )
    if "base_url" not in values:
        raise ValueError("Missing base_url setting for MOSAIC-s3-based pool")
    provided_level = values.get("compression_level", "none")
    if provided_level is None or provided_level == "none":
        compression_level = None
    else:
        compression_level = int(provided_level)
    return {
        "type": "mosaic-s3",
        "pool_name": values.get("pool_name", "mosaics"),
        "base_url": values["base_url"],
        "compression_level": compression_level,
        "anonymous": values.get("anonymous", False),
        "image_extension": values.get("image_extension", ""),
        "shard_max_size": values["shard_max_size"],
        "tmp_dir": values.get("tmp_dir"),
        "read_only": values.get("read_only", False),
        "boto3_config": values.get("boto3_config"),
    }


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
    shards_pools: Iterable[ShardsPool]
    shards_active_pool: str | None
    packer: Packer
    readers_cache_size: int | None


SETTINGS = frozenset(
    {
        "database",
        "shards_pools",
        "shards_active_pool",
        "packer",
        "readers_cache_size",
    }
)


def populate_default_settings(
    database: Optional[Database] = None,
    shards_pools: Iterable[ShardsPool] = (),
    shards_active_pool: str | None = None,
    packer: Optional[Packer] = None,
    throttler: Any = None,
    readers_cache_size: int | None = None,
) -> Winery:
    """Given some settings for a Winery objstorage, add all the appropriate
    default settings."""
    settings: Winery = {}

    if throttler is not None:
        logger.warning(
            "Throttling support has been removed; please update your configuration "
            "file (remove the throttler section)"
        )
    if database is not None:
        database = database_settings_with_defaults(database)
        settings["database"] = database

    pools: List[ShardsPool] = []
    for shards_pool in shards_pools:
        if shards_pool["type"] == "rbd":
            pools.append(rbd_shards_pool_settings_with_defaults(shards_pool))
        elif shards_pool["type"] == "directory":
            pools.append(directory_shards_pool_settings_with_defaults(shards_pool))
        elif shards_pool["type"] == "mosaic":
            pools.append(mosaic_pool_settings_with_defaults(shards_pool))
        elif shards_pool["type"] == "mosaic-s3":
            pools.append(mosaic_s3_pool_settings_with_defaults(shards_pool))
        else:
            raise ValueError(f"Unknown shards pool type: {shards_pool['type']}")
    if not pools:
        raise ValueError("At least one shards pool must be defined")
    settings["shards_pools"] = pools

    if shards_active_pool is not None:
        if shards_active_pool not in [pool["pool_name"] for pool in pools]:
            raise ValueError(
                f"shards_active_pool ({shards_active_pool}) "
                "must be the name of a given shards pool "
                f"({[pool['pool_name'] for pool in pools]})"
            )
        settings["shards_active_pool"] = shards_active_pool

    if packer is not None:
        packer = packer_settings_with_defaults(packer)
        settings["packer"] = packer

        if packer.get("clean_immediately"):
            logger.warning(
                "clean_immediately has been deprecated and is no longer "
                "used. Please update your configuration and setup."
            )
        if packer.get("pack_immediately"):
            logger.warning(
                "pack_immediately has been deprecated and is no longer "
                "used. Please update your configuration and setup."
            )

    if readers_cache_size is None:
        settings["readers_cache_size"] = SHARD_CACHE_DEFAULT_SIZE
    elif readers_cache_size == 0:
        logger.warning(
            "readers_cache_size should be a positive number. Using default size: %d",
            SHARD_CACHE_DEFAULT_SIZE,
        )
        settings["readers_cache_size"] = SHARD_CACHE_DEFAULT_SIZE
    elif readers_cache_size < 0:
        raise ValueError("readers_cache_size should be a positive number.")
    else:
        settings["readers_cache_size"] = readers_cache_size

    return settings
