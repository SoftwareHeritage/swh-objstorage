# Copyright (C) 2022-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from typing import Callable, Optional

from . import roshard, settings
from .rwshard import RWShard
from .sharedbase import ShardState, SharedBase
from .sleep import sleep_exponential
from .throttler import Throttler

logger = logging.getLogger(__name__)


def never_stop(_: int) -> bool:
    return False


def stop_after_shards(max_shards_packed: int) -> Callable[[int], bool]:
    def stop(shards_packed: int):
        return shards_packed >= max_shards_packed

    return stop


def shard_packer(
    database: settings.Database,
    shards: settings.Shards,
    shards_pool: settings.ShardsPool,
    throttler: settings.Throttler,
    packer: Optional[settings.Packer] = None,
    stop_packing: Callable[[int], bool] = never_stop,
    wait_for_shard: Callable[[int], None] = sleep_exponential(
        min_duration=5,
        factor=2,
        max_duration=60,
        message="No shards to pack",
    ),
) -> int:
    """Pack shards until the `stop_packing` function returns True.

    When no shards are available for packing, call the `wait_for_shard` function.

    Arguments:
      database: database settings (e.g. db connection string)
      shards: shards settings (e.g. max_size)
      shards_pool: shards pool settings (e.g. Ceph RBD settings)
      throttler: throttler settings
      packer: packer settings
      stop_packing: callback to determine whether the packer should exit
      wait_for_shard: sleep function called when no shards are available to be packed
    """

    all_settings = settings.populate_default_settings(
        database=database,
        shards=shards,
        shards_pool=shards_pool,
        throttler=throttler,
        packer=(packer or {}),
    )

    application_name = (
        all_settings["database"]["application_name"] or "Winery Shard Packer"
    )

    base = SharedBase(
        base_dsn=all_settings["database"]["db"],
        application_name=application_name,
    )

    shards_packed = 0
    waited_for_shards = 0
    while not stop_packing(shards_packed):
        locked = base.maybe_lock_one_shard(
            current_state=ShardState.FULL, new_state=ShardState.PACKING
        )

        if not locked:
            wait_for_shard(waited_for_shards)
            waited_for_shards += 1
            continue

        waited_for_shards = 0

        with locked:
            if locked.name is None:
                raise RuntimeError("No shard has been locked?")
            logger.info("shard_packer: Locked shard %s to pack", locked.name)
            ret = pack(
                shard=locked.name,
                base_dsn=all_settings["database"]["db"],
                packer_settings=all_settings["packer"],
                throttler_settings=all_settings["throttler"],
                shards_settings=all_settings["shards"],
                shards_pool_settings=all_settings["shards_pool"],
                shared_base=base,
            )
            if not ret:
                raise ValueError("Packing shard %s failed" % locked.name)
            shards_packed += 1

    return shards_packed


def pack(
    shard: str,
    base_dsn: str,
    packer_settings: settings.Packer,
    throttler_settings: Optional[settings.Throttler],
    shards_settings: settings.Shards,
    shards_pool_settings: settings.ShardsPool,
    shared_base: Optional[SharedBase] = None,
) -> bool:
    rw = RWShard(shard, shard_max_size=shards_settings["max_size"], base_dsn=base_dsn)

    count = rw.count()
    logger.info("Creating RO shard %s for %s objects", shard, count)
    throttler = Throttler.from_settings({"throttler": throttler_settings})
    pool = roshard.pool_from_settings(
        shards_settings=shards_settings, shards_pool_settings=shards_pool_settings
    )
    with roshard.ROShardCreator(
        name=shard,
        count=count,
        throttler=throttler,
        pool=pool,
        rbd_create_images=packer_settings["create_images"],
    ) as ro:
        logger.info("Created RO shard %s", shard)
        for i, (obj_id, content) in enumerate(rw.all()):
            ro.add(content, obj_id)
            if i % 100 == 99:
                logger.debug("RO shard %s: added %s/%s objects", shard, i + 1, count)

        logger.debug("RO shard %s: added %s objects, saving", shard, count)

    logger.info("RO shard %s: saved", shard)

    if not shared_base:
        shared_base = SharedBase(base_dsn=base_dsn)
    shared_base.shard_packing_ends(shard)
    if packer_settings["clean_immediately"]:
        cleanup_rw_shard(shard, shared_base=shared_base)
    return True


def rw_shard_cleaner(
    database: settings.Database,
    min_mapped_hosts: int,
    stop_cleaning: Callable[[int], bool] = never_stop,
    wait_for_shard: Callable[[int], None] = sleep_exponential(
        min_duration=5,
        factor=2,
        max_duration=60,
        message="No shards to clean up",
    ),
) -> int:
    """Clean up RW shards until the `stop_cleaning` function returns True.

    When no shards are available for packing, call the `wait_for_shard` function.

    Arguments:
      database: database settings (e.g. db connection string)
      min_mapped_hosts: how many hosts should have mapped the image read-only before
        cleaning it
      stop_cleaning: callback to determine whether the cleaner should exit
      wait_for_shard: sleep function called when no shards are available to be cleaned
    """
    database = settings.database_settings_with_defaults(database)

    base = SharedBase(base_dsn=database["db"])

    shards_cleaned = 0
    waited_for_shards = 0
    while not stop_cleaning(shards_cleaned):
        locked = base.maybe_lock_one_shard(
            current_state=ShardState.PACKED,
            new_state=ShardState.CLEANING,
            min_mapped_hosts=min_mapped_hosts,
        )

        if not locked:
            wait_for_shard(waited_for_shards)
            waited_for_shards += 1
            continue

        waited_for_shards = 0

        with locked:
            logger.info("rw_shard_cleaner: Locked shard %s to clean", locked.name)

            ret = cleanup_rw_shard(
                locked.name,
                base_dsn=database["db"],
                shared_base=base,
            )
            if not ret:
                raise ValueError("Cleaning shard %s failed" % locked.name)

            shards_cleaned += 1

    return shards_cleaned


def cleanup_rw_shard(shard, base_dsn=None, shared_base=None) -> bool:
    if shared_base is not None and not base_dsn:
        base_dsn = shared_base.dsn
    rw = RWShard(name=shard, shard_max_size=0, base_dsn=base_dsn)

    rw.drop()

    if not shared_base:
        shared_base = SharedBase(base_dsn=base_dsn)
    shared_base.set_shard_state(name=shard, new_state=ShardState.READONLY)

    return True


def deleted_objects_cleaner(
    base: SharedBase,
    pool: roshard.Pool,
    stop_running: Callable[[], bool],
):
    """Clean up deleted objects from RO shards and the shared database.

    This requires the ability to map RBD images in read-write mode. Images will be
    left mapped by this process as it is meant to be executed in a transient host
    dedicated to this purpose.

    Arguments:
      base_dsn: PostgreSQL dsn for the shared database
      pool: Ceph RBD pool for Winery shards
      stop_running: callback that returns True when the manager should stop running
    """
    count = 0
    for obj_id, shard_name, shard_state in base.deleted_objects():
        if stop_running():
            break
        if shard_state.readonly:
            roshard.ROShard.delete(pool, shard_name, obj_id)
        base.clean_deleted_object(obj_id)
        count += 1

    logger.info("Cleaned %d deleted objects", count)
