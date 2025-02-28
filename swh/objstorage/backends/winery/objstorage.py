# Copyright (C) 2022-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import partial
import logging
from multiprocessing import Process
from typing import Callable, Dict, Iterator, List, Optional

from swh.objstorage.constants import DEFAULT_LIMIT
from swh.objstorage.exc import ObjNotFoundError, ReadOnlyObjStorageError
from swh.objstorage.interface import CompositeObjId, ObjId
from swh.objstorage.objstorage import ObjStorage, timed

from . import settings
from .roshard import Pool, ROShard, ROShardCreator, ShardNotMapped
from .rwshard import RWShard
from .sharedbase import ShardState, SharedBase
from .sleep import sleep_exponential

logger = logging.getLogger(__name__)


class WineryObjStorage(ObjStorage):
    PRIMARY_HASH = "sha256"
    name: str = "winery"

    def __init__(
        self,
        database: settings.Database,
        shards: settings.Shards,
        shards_pool: settings.ShardsPool,
        throttler: settings.Throttler,
        packer: Optional[settings.Packer] = None,
        readonly: bool = False,
        allow_delete: bool = False,
        name: str = "winery",
    ) -> None:
        super().__init__(allow_delete=allow_delete, name=name)

        self.settings, legacy_kwargs = settings.populate_default_settings(
            database=database,
            shards=shards,
            shards_pool=shards_pool,
            throttler=throttler,
            packer=(packer or {}),
        )

        self.reader = WineryReader(
            throttler_settings=self.settings["throttler"], **legacy_kwargs
        )

        if readonly:
            self.writer = None
        else:
            self.writer = WineryWriter(
                packer_settings=self.settings["packer"],
                throttler_settings=self.settings["throttler"],
                **legacy_kwargs,
            )

    @timed
    def get(self, obj_id: ObjId) -> bytes:
        try:
            return self.reader.get(self._hash(obj_id))
        except ObjNotFoundError as exc:
            # re-raise exception with the passed obj_id instead of the internal winery obj_id.
            raise ObjNotFoundError(obj_id) from exc

    def check_config(self, *, check_write: bool) -> bool:
        return True

    @timed
    def __contains__(self, obj_id: ObjId) -> bool:
        return self._hash(obj_id) in self.reader

    @timed
    def add(self, content: bytes, obj_id: ObjId, check_presence: bool = True) -> None:
        if not self.writer:
            raise ReadOnlyObjStorageError("add")
        internal_obj_id = self._hash(obj_id)
        if check_presence and internal_obj_id in self.reader:
            return
        self.writer.add(content, internal_obj_id)

    def delete(self, obj_id: ObjId):
        if not self.writer:
            raise ReadOnlyObjStorageError("delete")
        if not self.allow_delete:
            raise PermissionError("Delete is not allowed.")
        try:
            return self.writer.delete(self._hash(obj_id))
        # Re-raise ObjNotFoundError with the full object id
        except ObjNotFoundError as exc:
            raise ObjNotFoundError(obj_id) from exc

    def _hash(self, obj_id: ObjId) -> bytes:
        return obj_id[self.PRIMARY_HASH]

    def __iter__(self) -> Iterator[CompositeObjId]:
        if self.PRIMARY_HASH != "sha256":
            raise ValueError(f"Unknown primary hash {self.PRIMARY_HASH}")
        for signature in self.reader.list_signatures():
            yield {"sha256": signature}

    def list_content(
        self,
        last_obj_id: Optional[ObjId] = None,
        limit: Optional[int] = DEFAULT_LIMIT,
    ) -> Iterator[CompositeObjId]:
        if self.PRIMARY_HASH != "sha256":
            raise ValueError(f"Unknown primary hash {self.PRIMARY_HASH}")

        after_id: Optional[bytes] = None
        if last_obj_id:
            after_id = self._hash(last_obj_id)

        for signature in self.reader.list_signatures(after_id=after_id, limit=limit):
            yield {"sha256": signature}

    def on_shutdown(self):
        if self.writer:
            self.writer.on_shutdown()


class WineryReader:
    def __init__(self, throttler_settings: settings.Throttler, **kwargs):
        self.args = kwargs
        self.throttler_settings = throttler_settings
        self.base = SharedBase(**self.args)
        self.ro_shards: Dict[str, ROShard] = {}
        self.rw_shards: Dict[str, RWShard] = {}

    def __contains__(self, obj_id):
        return self.base.contains(obj_id)

    def list_signatures(
        self, after_id: Optional[bytes] = None, limit: Optional[int] = None
    ) -> Iterator[bytes]:
        yield from self.base.list_signatures(after_id, limit)

    def roshard(self, name) -> Optional[ROShard]:
        if name not in self.ro_shards:
            try:
                shard = ROShard(
                    name=name, throttler_settings=self.throttler_settings, **self.args
                )
            except ShardNotMapped:
                return None
            self.ro_shards[name] = shard
            if name in self.rw_shards:
                del self.rw_shards[name]
        return self.ro_shards[name]

    def rwshard(self, name) -> RWShard:
        if name not in self.rw_shards:
            shard = RWShard(name, **self.args)
            self.rw_shards[name] = shard
        return self.rw_shards[name]

    def get(self, obj_id: bytes) -> bytes:
        shard_info = self.base.get(obj_id)
        if shard_info is None:
            raise ObjNotFoundError(obj_id)
        name, state = shard_info
        content: Optional[bytes] = None
        if state.image_available:
            roshard = self.roshard(name)
            if roshard:
                content = roshard.get(obj_id)
        if content is None:
            rwshard = self.rwshard(name)
            content = rwshard.get(obj_id)
        if content is None:
            raise ObjNotFoundError(obj_id)
        return content


def pack(
    shard,
    packer_settings: settings.Packer,
    throttler_settings: settings.Throttler,
    shared_base=None,
    **kwargs,
) -> bool:
    rw = RWShard(shard, **kwargs)

    count = rw.count()
    logger.info("Creating RO shard %s for %s objects", shard, count)
    with ROShardCreator(
        shard, count, throttler_settings=throttler_settings, **kwargs
    ) as ro:
        logger.info("Created RO shard %s", shard)
        for i, (obj_id, content) in enumerate(rw.all()):
            ro.add(content, obj_id)
            if i % 100 == 99:
                logger.debug("RO shard %s: added %s/%s objects", shard, i + 1, count)

        logger.debug("RO shard %s: added %s objects, saving", shard, count)

    logger.info("RO shard %s: saved", shard)

    if not shared_base:
        shared_base = SharedBase(**kwargs)
    shared_base.shard_packing_ends(shard)
    if packer_settings["clean_immediately"]:
        cleanup_rw_shard(shard, shared_base=shared_base, **kwargs)
    return True


def cleanup_rw_shard(shard, shared_base=None, **kwargs) -> bool:
    rw = RWShard(shard, **{"shard_max_size": 0, **kwargs})

    rw.drop()

    if not shared_base:
        shared_base = SharedBase(**kwargs)
    shared_base.set_shard_state(name=shard, new_state=ShardState.READONLY)

    return True


class WineryWriter:
    def __init__(
        self,
        packer_settings: settings.Packer,
        throttler_settings: settings.Throttler,
        rwshard_idle_timeout: float = 300,
        **kwargs,
    ):
        self.packer_settings = packer_settings
        self.throttler_settings = throttler_settings
        self.args = kwargs
        self.base = SharedBase(**self.args)
        self.shards_filled: List[str] = []
        self.packers: List[Process] = []
        self._shard: Optional[RWShard] = None
        self.idle_timeout = rwshard_idle_timeout

    def release_shard(
        self,
        shard: Optional[RWShard] = None,
        from_idle_handler: bool = False,
        new_state: ShardState = ShardState.STANDBY,
    ):
        """Release the currently locked shard"""
        if not shard:
            shard = self._shard

        if not shard:
            return

        logger.debug("WineryWriter releasing shard %s", shard.name)

        self.base.set_shard_state(new_state=new_state, name=shard.name)
        if not from_idle_handler:
            logger.debug("Shard released, disabling idle handler")
            shard.disable_idle_handler()
        self._shard = None

    @property
    def shard(self):
        """Lock a shard to be able to use it. Release it after :attr:`idle_timeout`."""
        if not self._shard:
            self._shard = RWShard(
                self.base.locked_shard,
                idle_timeout_cb=partial(self.release_shard, from_idle_handler=True),
                idle_timeout=self.idle_timeout,
                **self.args,
            )
            logger.debug(
                "WineryBase: locked RWShard %s, releasing it in %s",
                self._shard.name,
                self.idle_timeout,
            )
        return self._shard

    def add(self, content: bytes, obj_id: bytes) -> None:
        with self.base.pool.connection() as db, db.transaction():
            shard = self.base.record_new_obj_id(db, obj_id)
            if shard != self.base.locked_shard_id:
                #  this object is the responsibility of another shard
                return

            self.shard.add(db, obj_id, content)

        if self.shard.is_full():
            filled_name = self.shard.name
            self.release_shard(new_state=ShardState.FULL)
            self.shards_filled.append(filled_name)
            if self.packer_settings["pack_immediately"]:
                self.pack(filled_name)

    def delete(self, obj_id: bytes):
        shard_info = self.base.get(obj_id)
        if shard_info is None:
            raise ObjNotFoundError(obj_id)
        name, state = shard_info
        # We only care about RWShard for now. ROShards will be
        # taken care in a batch job.
        if not state.image_available:
            rwshard = RWShard(name, **self.args)
            try:
                rwshard.delete(obj_id)
            except KeyError:
                logger.warning(
                    "Shard %s does not seem to know about object %s, but we "
                    "had an entry in SharedBase (which is going to "
                    "be removed just now)",
                    rwshard.name,
                    obj_id,
                )
        self.base.delete(obj_id)
        return True

    def check(self, obj_id: ObjId) -> None:
        # load all shards packing == True and not locked (i.e. packer
        # was interrupted for whatever reason) run pack for each of them
        pass

    def pack(self, shard_name: str):
        self.base.shard_packing_starts(shard_name)
        p = Process(
            target=pack,
            kwargs={
                "shard": shard_name,
                "packer_settings": self.packer_settings,
                "throttler_settings": self.throttler_settings,
                **self.args,
            },
        )
        p.start()
        self.packers.append(p)

    def on_shutdown(self):
        self.release_shard()

    def __del__(self):
        for p in getattr(self, "packers", []):
            if not p.is_alive():
                continue
            logger.warning("Killing packer %s", p)
            p.kill()
            p.join()


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

    all_settings, legacy_kwargs = settings.populate_default_settings(
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
            logger.info("shard_packer: Locked shard %s to pack", locked.name)
            ret = pack(
                locked.name,
                packer_settings=all_settings["packer"],
                throttler_settings=all_settings["throttler"],
                shared_base=base,
                **legacy_kwargs,
            )
            if not ret:
                raise ValueError("Packing shard %s failed" % locked.name)
            shards_packed += 1

    return shards_packed


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
    for obj_id, shard_name, shard_state in base.deleted_objects():
        if stop_running():
            break
        if shard_state.readonly:
            ROShard.delete(pool, shard_name, obj_id)
        base.clean_deleted_object(obj_id)
        count += 1

    logger.info("Cleaned %d deleted objects", count)
