# Copyright (C) 2022-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from time import monotonic
from typing import Callable, Iterable, Optional, Tuple

from psycopg.errors import UniqueViolation

from swh.core.statsd import statsd
from swh.core.utils import grouper
from swh.shard import Shard
from swh.shard.cli import NULLKEY

from . import roshard, settings
from .pools import Pool, pool_from_settings
from .rwshard import RWShard
from .sharedbase import ShardState, SharedBase
from .sleep import sleep_exponential

logger = logging.getLogger(__name__)


class AbortOperation(Exception):
    pass


def never_stop(_: int) -> bool:
    return False


def stop_after_shards(max_shards_packed: int) -> Callable[[int], bool]:
    def stop(shards_packed: int):
        return shards_packed >= max_shards_packed

    return stop


def shard_packer(
    database: settings.Database,
    shards: settings.Shards,
    shards_pools: Iterable[settings.ShardsPool],
    shards_active_pool: str | None,
    packer: Optional[settings.Packer] = None,
    stop_packing: Callable[[int], bool] = never_stop,
    abort_packing: Callable[[int], bool] = never_stop,
    wait_for_shard: Callable[[int], None] = sleep_exponential(
        min_duration=5,
        factor=2,
        max_duration=60,
        message="No shards to pack",
    ),
    **kwargs,
) -> int:
    """Pack shards until the `stop_packing` function returns True.

    When no shards are available for packing, call the `wait_for_shard` function.

    Arguments:
      database: database settings (e.g. db connection string)
      shards: shards settings (e.g. max_size)
      shards_pool: shards pool settings (e.g. Ceph RBD settings)
      shards_active_pool: the pool for which packing is to be done; if None, pack
        for all pools
      packer: packer settings
      stop_packing: callback to determine whether the packer should exit
      abort_packing: callback to determine whether the packer should abort
      wait_for_shard: sleep function called when no shards are available to be packed
    """

    all_settings = settings.populate_default_settings(
        database=database,
        shards=shards,
        shards_pools=shards_pools,
        shards_active_pool=shards_active_pool,
        packer=(packer or {}),
    )
    application_name = (
        all_settings["database"]["application_name"] or "Winery Shard Packer"
    )

    base = SharedBase(
        base_dsn=all_settings["database"]["db"],
        application_name=application_name,
        active_pool_name=shards_active_pool,
    )

    shards_packed = 0
    waited_for_shards = 0
    while not stop_packing(shards_packed):
        locked = base.maybe_lock_one_shard(
            current_state=ShardState.FULL,
            new_state=ShardState.PACKING,
            from_pool=shards_active_pool,
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
            pool_name = base.get_shard_pool(locked.name)
            for pool_cfg in all_settings["shards_pools"]:
                if pool_cfg["pool_name"] == pool_name:
                    break
            else:
                raise ValueError("Missing or unknown active pool")
            ret = pack(
                shard=locked.name,
                base_dsn=all_settings["database"]["db"],
                packer_settings=all_settings["packer"],
                shards_settings=all_settings["shards"],
                shards_pool_settings=pool_cfg,
                shared_base=base,
                abort_packing=abort_packing,
            )
            if not ret:
                raise ValueError("Packing shard %s failed" % locked.name)
            shards_packed += 1

    return shards_packed


def pack(
    shard: str,
    base_dsn: str,
    packer_settings: settings.Packer,
    shards_settings: settings.Shards,
    shards_pool_settings: settings.ShardsPool,
    shared_base: Optional[SharedBase] = None,
    abort_packing: Callable[[int], bool] = never_stop,
) -> bool:
    rw = RWShard(shard, shard_max_size=shards_settings["max_size"], base_dsn=base_dsn)
    if not shared_base:
        shared_base = SharedBase(base_dsn=base_dsn)

    count = rw.count()
    logger.info("Creating RO shard %s for %s objects", shard, count)
    pool = pool_from_settings(
        shards_settings=shards_settings,
        shards_pool_settings=shards_pool_settings,
    )
    statsd.gauge(
        "swh_objstorage_winery_packer_shard_max_size_bytes", shards_settings["max_size"]
    )
    tags = {"pool_name": pool.pool_name}
    t0 = monotonic()
    with statsd.timed("swh_objstorage_winery_packer_seconds", tags=tags):
        with roshard.ROShardCreator(
            name=shard,
            count=count,
            pool=pool,
            rbd_create_images=packer_settings["create_images"],
        ) as ro:
            logger.info("Created RO shard %s", shard)
            for i, (obj_id, content) in enumerate(rw.all()):
                ro.add(content, obj_id)
                if abort_packing(i):
                    logger.info("Aborting packing of %s", shard)
                    raise AbortOperation("Packing shard %s aborted" % shard)

                if i % 100 == 99:
                    logger.debug(
                        "RO shard %s: added %s/%s objects", shard, i + 1, count
                    )
                statsd.increment("swh_objstorage_winery_packer_objects", tags=tags)
                statsd.increment(
                    "swh_objstorage_winery_packer_volume_bytes",
                    value=len(content),
                    tags=tags,
                )

            logger.debug("RO shard %s: added %s objects, saving", shard, count)

    logger.info("RO shard %s: saved (in %ds)", shard, monotonic() - t0)

    shared_base.shard_packing_ends(shard)
    if packer_settings.get("clean_immediately"):
        logger.warning(
            "clean_immediately has been disabled. Please use a "
            "'swh objstorage winery rw-shard-cleaner' service instead. "
            "Cleaning will NOT be executed now."
        )
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
                base_dsn=base.dsn,
            )
            if not ret:
                raise ValueError("Cleaning shard %s failed" % locked.name)

            shards_cleaned += 1

    return shards_cleaned


def cleanup_rw_shard(shard, base_dsn) -> bool:
    rw = RWShard(name=shard, shard_max_size=0, base_dsn=base_dsn)
    rw.drop()

    shared_base = SharedBase(base_dsn=base_dsn)
    shared_base.set_shard_state(name=shard, new_state=ShardState.READONLY)

    return True


def deleted_objects_cleaner(
    base: SharedBase,
    pool: Pool,
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
    for obj_id, shard_name, shard_state, _ in base.deleted_objects(
        pool_name=pool.pool_name
    ):
        if stop_running():
            break
        if shard_state.readonly:
            roshard.ROShard.delete(pool, shard_name, obj_id)
        base.clean_deleted_object(obj_id)
        count += 1

    logger.info("Cleaned %d deleted objects", count)


def import_ro_shards(
    base: SharedBase, pool: Pool, shards: Iterable[str] | None = None
) -> Tuple[int, int]:
    """Import existing shard files in the winery database."""
    n_obj = 0
    n_shard = 0
    if not shards:
        shards = pool.image_list()
    for imgname in shards:
        with Shard(pool.image_path(imgname)) as s:
            if base.get_shard_state(name=imgname) is not None:
                logger.info(f"Shard {imgname} already exists, skipping")
                continue
            try:
                base._locked_shard = base.create_shard(
                    ShardState.PACKING,
                    name=imgname,
                    pool_name=pool.pool_name,
                )
            except UniqueViolation:
                # Should not happen, but sh*t happen, so better safe than sorry
                # The shard already exists in the winery DB, skip it
                logger.info(f"Shard {imgname} already exists, skipping")
                # TODO: check stored entries match?
                continue
            with base.pool.connection() as db, db.transaction():
                for keys in grouper(s, 10000):
                    keys = [key for key in keys if key != NULLKEY]
                    known = [key for key in keys if base.contains(key)]
                    if known:
                        logger.info(
                            "Keys %s are already known, skipping",
                            [key.hex() for key in known],
                        )
                    base.record_new_obj_ids(
                        db, [key for key in keys if key not in known]
                    )
                    n_obj += len(keys) - len(known)

            base.shard_packing_ends(imgname)
            n_shard += 1
            base.set_shard_state(name=imgname, new_state=ShardState.READONLY)
            pool.image_map(imgname, options="ro")

    return n_obj, n_shard
