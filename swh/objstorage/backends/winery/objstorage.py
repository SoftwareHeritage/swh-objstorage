# Copyright (C) 2022-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import partial
import logging
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from swh.objstorage.constants import LiteralPrimaryHash
from swh.objstorage.exc import ObjNotFoundError, ReadOnlyObjStorageError
from swh.objstorage.interface import HashDict
from swh.objstorage.metrics import timed
from swh.objstorage.objstorage import ObjStorage

from . import settings
from .housekeeping import pack
from .pools import Pool, pool_from_settings
from .roshard import ROShard, ShardNotMapped
from .rwshard import RWShard
from .sharedbase import ShardState, SharedBase

logger = logging.getLogger(__name__)


class WineryObjStorage(ObjStorage):
    primary_hash: LiteralPrimaryHash = "sha256"
    name: str = "winery"

    def __init__(
        self,
        database: settings.Database,
        shards: settings.Shards,
        shards_pool: settings.ShardsPool,
        packer: Optional[settings.Packer] = None,
        readonly: bool = False,
        allow_delete: bool = False,
        name: str = "winery",
    ) -> None:
        super().__init__(allow_delete=allow_delete, name=name)
        if self.primary_hash != "sha256":
            raise TypeError("Winery backend only support the sha256 primary hash")

        self.settings = settings.populate_default_settings(
            database=database,
            shards=shards,
            shards_pool=shards_pool,
            packer=(packer or {}),
        )

        self.pool = pool_from_settings(
            shards_settings=self.settings["shards"],
            shards_pool_settings=self.settings["shards_pool"],
        )
        self.reader: WineryReader = WineryReader(
            pool=self.pool, database=self.settings["database"]
        )

        self.writer: Optional[WineryWriter] = None
        if not readonly:
            self.writer = WineryWriter(
                packer_settings=self.settings["packer"],
                shards_settings=self.settings["shards"],
                shards_pool_settings=self.settings["shards_pool"],
                database_settings=self.settings["database"],
            )

    @timed
    def get(self, obj_id: HashDict) -> bytes:
        try:
            return self.reader.get(self._hash(obj_id))
        except ObjNotFoundError as exc:
            # re-raise exception with the passed obj_id instead of the internal winery obj_id.
            raise ObjNotFoundError(obj_id) from exc

    def check_config(self, *, check_write: bool) -> bool:
        return True

    @timed
    def contains(self, obj_id: HashDict) -> bool:
        return self._hash(obj_id) in self.reader

    @timed
    def add(
        self, content: bytes, obj_id: HashDict, check_presence: bool = True
    ) -> None:
        self._add_batch([(obj_id, content)])

    @timed
    def add_batch(
        self, contents: Iterable[tuple[HashDict, bytes]], check_presence: bool = True
    ) -> Dict:
        """``contents`` should be pairs of ``(obj_id, content)``"""
        return self._add_batch(contents, check_presence)

    def _add_batch(
        self, contents: Iterable[tuple[HashDict, bytes]], check_presence: bool = True
    ) -> Dict:
        """Same as ``add_batch``, but not wrapped by ``@timed``, so ``add()`` is not
        double-counted"""
        if not self.writer:
            raise ReadOnlyObjStorageError("add")
        hashed_contents = (
            (self._hash(obj_id), content) for (obj_id, content) in contents
        )
        if check_presence:
            # filter out contents that already exist
            hashed_contents = (
                (internal_obj_id, content)
                for (internal_obj_id, content) in hashed_contents
                if internal_obj_id not in self.reader
            )
        hashed_contents_list = list(hashed_contents)
        if hashed_contents_list:
            return self.writer.add_batch(hashed_contents_list)
        return {"object:add": 0, "object:add:bytes": 0}

    def delete(self, obj_id: HashDict):
        if not self.writer:
            raise ReadOnlyObjStorageError("delete")
        if not self.allow_delete:
            raise PermissionError("Delete is not allowed.")
        try:
            return self.writer.delete(self._hash(obj_id))
        # Re-raise ObjNotFoundError with the full object id
        except ObjNotFoundError as exc:
            raise ObjNotFoundError(obj_id) from exc

    def _hash(self, obj_id: HashDict) -> bytes:
        return obj_id[self.primary_hash]

    def on_shutdown(self):
        self.reader.on_shutdown()
        if self.writer:
            self.writer.on_shutdown()


class WineryReader:
    def __init__(self, pool: Pool, database: settings.Database):
        self.pool = pool
        self.base = SharedBase(
            base_dsn=database["db"], application_name=database["application_name"]
        )
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
                    name=name,
                    pool=self.pool,
                )
            except ShardNotMapped:
                return None
            self.ro_shards[name] = shard
            if name in self.rw_shards:
                del self.rw_shards[name]
        return self.ro_shards[name]

    def rwshard(self, name) -> RWShard:
        if name not in self.rw_shards:
            shard = RWShard(
                name, shard_max_size=0, base_dsn=self.base.dsn, readonly=True
            )
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

    def on_shutdown(self):
        for shard in self.ro_shards.values():
            shard.close()
        self.ro_shards = {}
        self.rw_shards = {}


class WineryWriter:
    def __init__(
        self,
        packer_settings: settings.Packer,
        shards_settings: settings.Shards,
        shards_pool_settings: settings.ShardsPool,
        database_settings: settings.Database,
    ):
        self.packer_settings = packer_settings
        self.shards_settings = shards_settings
        self.shards_pool_settings = shards_pool_settings
        self.base = SharedBase(
            base_dsn=database_settings["db"],
            application_name=database_settings["application_name"],
        )
        self.shards_filled: List[str] = []
        self._shard: Optional[RWShard] = None
        self.idle_timeout = shards_settings.get("rw_idle_timeout", 300)

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
                name=self.base.locked_shard,
                base_dsn=self.base.dsn,
                shard_max_size=self.shards_settings["max_size"],
                idle_timeout_cb=partial(self.release_shard, from_idle_handler=True),
                idle_timeout=self.idle_timeout,
            )
            logger.debug(
                "WineryBase: locked RWShard %s, releasing it in %s",
                self._shard.name,
                self.idle_timeout,
            )
        return self._shard

    def add(self, content: bytes, obj_id: bytes) -> None:
        self.add_batch([(obj_id, content)])

    def add_batch(self, contents: List[Tuple[bytes, bytes]]) -> Dict:
        """``contents`` should be pairs of ``(obj_id, content)``"""
        with self.base.pool.connection() as db, db.transaction():
            shards = self.base.record_new_obj_ids(
                db, [obj_id for (obj_id, _content) in contents]
            )
            contents = [
                (obj_id, content)
                for (obj_id, content) in contents
                # if not equal, this object is the responsibility of another shard:
                if shards[obj_id] == self.base.locked_shard_id
            ]

            stats = self.shard.add_batch(db, contents)

        if self.shard.is_full():
            filled_name = self.shard.name
            self.release_shard(new_state=ShardState.FULL)
            self.shards_filled.append(filled_name)
            if self.packer_settings["pack_immediately"]:
                logger.warning(
                    "pack_immediately has been disabled. Please use a "
                    "'swh objstorage winery packer' service instead. "
                    "Packing will NOT be executed now."
                )

        return stats

    def delete(self, obj_id: bytes):
        shard_info = self.base.get(obj_id)
        if shard_info is None:
            raise ObjNotFoundError(obj_id)
        name, state = shard_info
        # We only care about RWShard for now. ROShards will be
        # taken care in a batch job.
        if not state.image_available:
            rwshard = RWShard(name, shard_max_size=0, base_dsn=self.base.dsn)
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

    def check(self, obj_id: HashDict) -> None:
        # load all shards packing == True and not locked (i.e. packer
        # was interrupted for whatever reason) run pack for each of them
        pass

    def pack(self, shard_name: str):
        self.base.shard_packing_starts(shard_name)
        return pack(
            shard=shard_name,
            base_dsn=self.base.dsn,
            packer_settings=self.packer_settings,
            shards_settings=self.shards_settings,
            shards_pool_settings=self.shards_pool_settings,
        )

    def on_shutdown(self):
        self.release_shard()
