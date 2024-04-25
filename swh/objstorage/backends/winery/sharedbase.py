# Copyright (C) 2021-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import ExitStack
from enum import Enum
import logging
from types import TracebackType
from typing import Iterator, Optional, Set, Tuple, Type
import uuid

import psycopg
import psycopg.errors

from .database import Database

WRITER_UUID = uuid.uuid4()

logger = logging.getLogger(__name__)


class ShardState(Enum):
    STANDBY = "standby"
    WRITING = "writing"
    FULL = "full"
    PACKING = "packing"
    PACKED = "packed"
    CLEANING = "cleaning"
    READONLY = "readonly"

    @property
    def locked(self):
        return self not in {self.STANDBY, self.FULL, self.PACKED, self.READONLY}

    @property
    def image_available(self):
        return self in {self.PACKED, self.CLEANING, self.READONLY}

    @property
    def readonly(self):
        return self in {self.CLEANING, self.READONLY}


class SignatureState(Enum):
    INFLIGHT = "inflight"
    PRESENT = "present"
    DELETED = "deleted"


class TemporaryShardLocker:
    """Opportunistically lock a shard, and provide a context manager to unlock
    the shard if an operation fails.

    Use this through the :meth:`SharedBase.maybe_lock_one_shard` method.
    """

    def __init__(
        self,
        base: "SharedBase",
        current_state: ShardState,
        new_state: ShardState,
        min_mapped_hosts: int = 0,
    ) -> None:
        self.base = base
        self.previous_state = current_state
        self.name: Optional[str] = None
        self.id: Optional[int] = None
        locked = self.base.lock_one_shard(
            current_state=current_state,
            new_state=new_state,
            min_mapped_hosts=min_mapped_hosts,
        )
        if locked:
            self.name, self.id = locked

    def __bool__(self) -> bool:
        return self.name is not None

    def __enter__(self) -> "TemporaryShardLocker":
        if not self:
            raise ValueError("No shard was locked")
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[Exception]],
        exc_value: Optional[Exception],
        traceback: Optional[TracebackType],
    ) -> None:
        """If an exception has been raised, restore the shard to its previous state"""
        if not self:
            return

        if not exc_type:
            return

        try:
            self.base.set_shard_state(
                name=self.name, new_state=self.previous_state, check_locker=True
            )
        except Exception:
            logger.warning("Could not unlock shard %s:", self.name, exc_info=True)


class SharedBase(Database):
    current_version: int = 1

    def __init__(
        self, base_dsn: str, application_name: str = "SWH Winery SharedBase", **kwargs
    ) -> None:
        super().__init__(
            dsn=base_dsn,
            application_name=application_name,
        )
        self._locked_shard: Optional[Tuple[str, int]] = None

        logger.debug("SharedBase %s: instantiated", WRITER_UUID)

    def uninit(self):
        if self._locked_shard is not None:
            self.set_shard_state(new_state=ShardState.STANDBY)
        self._locked_shard = None
        super().uninit()

    @property
    def locked_shard(self) -> str:
        self.set_locked_shard()

        assert self._locked_shard, "failed to lock a shard"
        return self._locked_shard[0]

    @property
    def locked_shard_id(self) -> int:
        self.set_locked_shard()

        assert self._locked_shard, "failed to lock a shard"
        return self._locked_shard[1]

    def set_locked_shard(self) -> None:
        if self._locked_shard is not None:
            return

        locked = self.lock_one_shard(
            current_state=ShardState.STANDBY, new_state=ShardState.WRITING
        )
        if locked is not None:
            self._locked_shard = locked
        else:
            if logger.isEnabledFor(logging.DEBUG):
                import traceback

                stack = traceback.extract_stack()
                for item in stack[::-1]:
                    if item.filename != __file__:
                        logger.debug(
                            "Creating new shard from file %s, line %d, function %s: %s",
                            item.filename,
                            item.lineno,
                            item.name,
                            item.line,
                        )
                        break
            self._locked_shard = self.create_shard(new_state=ShardState.WRITING)

        return

    def lock_one_shard(
        self,
        current_state: ShardState,
        new_state: ShardState,
        min_mapped_hosts: int = 0,
    ) -> Optional[Tuple[str, int]]:
        """Lock one shard in `current_state`, putting it into `new_state`. Only
        lock a shard if it has more than `min_mapped_hosts` hosts that have
        registered as having mapped the shard.
        """

        if not new_state.locked:
            raise ValueError(f"{new_state} is not a locked state")

        with self.pool.connection() as db, db.transaction():
            with db.cursor() as c:
                # run the next two statements in a transaction
                c.execute(
                    """\
                    SELECT name
                    FROM shards
                    WHERE
                      state = %s
                      AND COALESCE(ARRAY_LENGTH(mapped_on_hosts_when_packed, 1), 0) >= %s
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    """,
                    (current_state.value, min_mapped_hosts),
                )
                result = c.fetchone()
                if result is None:
                    return None
                shard_name = result[0]
                try:
                    c.execute(
                        """\
                        UPDATE shards
                        SET state = %s, locker_ts = now(), locker = %s
                        WHERE name = %s
                        RETURNING name, id
                        """,
                        (
                            new_state.value,
                            WRITER_UUID,
                            shard_name,
                        ),
                    )
                except psycopg.Error:
                    logger.exception(
                        "SharedBase %s: shard %s failed to lock",
                        WRITER_UUID,
                        shard_name,
                    )
                    return None
                else:
                    logger.debug(
                        "SharedBase %s: shard %s locked", WRITER_UUID, shard_name
                    )
                    return c.fetchone()

    def maybe_lock_one_shard(
        self,
        current_state: ShardState,
        new_state: ShardState,
        min_mapped_hosts: int = 0,
    ) -> TemporaryShardLocker:
        """Opportunistically lock a shard, and, if a shard was locked, provide a
        context manager to rollback the locking on failure.

        Example::

           locked = base.maybe_lock_one_shard(
               current_state=ShardState.FULL,
               new_state=ShardState.PACKING,
           )
           if not locked:
               wait_a_minute()
               return

           with locked:
               do_something_with_locked_shard(locked.name)

        If ``do_something_with_locked_shard`` fails, the shard will be moved
        back to the ``current_state`` on exit.
        """
        return TemporaryShardLocker(
            base=self,
            current_state=current_state,
            new_state=new_state,
            min_mapped_hosts=min_mapped_hosts,
        )

    def set_shard_state(
        self,
        new_state: ShardState,
        set_locker: bool = False,
        check_locker: bool = False,
        name: Optional[str] = None,
        db: Optional[psycopg.Connection] = None,
    ):
        reset_locked_shard = False
        if not name:
            if not self._locked_shard:
                raise ValueError("Can't set shard state, no shard specified or locked")
            name = self._locked_shard[0]
            reset_locked_shard = True

        with ExitStack() as stack:
            if not db:
                db = stack.enter_context(self.pool.connection())

            assert isinstance(db, psycopg.Connection)

            c = db.execute(
                """\
                UPDATE shards
                SET
                  locker = %s,
                  locker_ts = (CASE WHEN %s THEN NOW() ELSE NULL END),
                  state = %s
                WHERE name = %s AND (CASE WHEN %s THEN locker = %s ELSE TRUE END)
                """,
                (
                    WRITER_UUID if set_locker else None,
                    set_locker,
                    new_state.value,
                    name,
                    check_locker,
                    WRITER_UUID,
                ),
            )
            affected = c.rowcount
            if affected != 1:
                raise ValueError(
                    "set_pack_state(%s) affected %s rows, expected 1" % (name, affected)
                )

            logger.debug(
                "SharedBase %s: shard %s moved into state %s",
                WRITER_UUID,
                name,
                new_state,
            )

        if reset_locked_shard and not new_state.locked:
            self._locked_shard = None

    def create_shard(self, new_state: ShardState) -> Tuple[str, int]:
        name = uuid.uuid4().hex
        #
        # ensure the first character is not a number so it can be used as a
        # database name.
        #
        name = "i" + name[1:]
        with self.pool.connection() as db, db.cursor() as c:
            c.execute(
                """\
                INSERT INTO shards
                  (name, state, locker, locker_ts)
                VALUES
                  (%s, %s, %s, NOW())
                RETURNING name, id""",
                (name, new_state.value, WRITER_UUID),
            )
            res = c.fetchone()
            if res is None:
                raise RuntimeError(
                    f"Writer {WRITER_UUID} failed to create shard with name {name}"
                )

            logger.debug(
                "SharedBase %s: shard %s created (and locked)", WRITER_UUID, res[0]
            )
            return res

    def shard_packing_starts(self, name: str):
        with self.pool.connection() as db, db.transaction():
            with db.cursor() as c:
                c.execute(
                    "SELECT state FROM shards WHERE name=%s FOR UPDATE SKIP LOCKED",
                    (name,),
                )
                returned = c.fetchone()
                if not returned:
                    raise ValueError("Could not get shard state for %s" % name)
                state = ShardState(returned[0])
                if state != ShardState.FULL:
                    raise ValueError(
                        "Cannot pack shard in state %s, expected ShardState.FULL"
                        % state
                    )

                logger.debug(
                    "SharedBase %s: shard %s starts packing", WRITER_UUID, name
                )
                self.set_shard_state(
                    name=name,
                    new_state=ShardState.PACKING,
                    set_locker=True,
                    check_locker=False,
                    db=db,
                )

    def shard_packing_ends(self, name):
        with self.pool.connection() as db, db.transaction():
            with db.cursor() as c:
                c.execute(
                    "SELECT state FROM shards WHERE name=%s FOR UPDATE SKIP LOCKED",
                    (name,),
                )
                returned = c.fetchone()
                if not returned:
                    raise ValueError("Could not get shard state for %s" % name)
                state = ShardState(returned[0])
                if state != ShardState.PACKING:
                    raise ValueError(
                        "Cannot finalize packing for shard in state %s,"
                        " expected ShardState.PACKING" % state
                    )

                logger.debug("SharedBase %s: shard %s done packing", WRITER_UUID, name)
                self.set_shard_state(
                    name=name,
                    new_state=ShardState.PACKED,
                    set_locker=False,
                    check_locker=False,
                    db=db,
                )

    def get_shard_info(self, id: int) -> Optional[Tuple[str, ShardState]]:
        with self.pool.connection() as db, db.cursor() as c:
            c.execute("SELECT name, state FROM shards WHERE id = %s", (id,))
            row = c.fetchone()
            if not row:
                return None
            return (row[0], ShardState(row[1]))

    def get_shard_state(self, name: str) -> Optional[ShardState]:
        with self.pool.connection() as db, db.cursor() as c:
            c.execute("SELECT state FROM shards WHERE name = %s", (name,))
            row = c.fetchone()
            if not row:
                return None
            return ShardState(row[0])

    def list_shards(self) -> Iterator[Tuple[str, ShardState]]:
        with self.pool.connection() as db, db.cursor() as c:
            c.execute("SELECT name, state FROM shards")
            for row in c:
                yield row[0], ShardState(row[1])

    def count_objects(self, name: Optional[str] = None) -> Optional[int]:
        if not name:
            if not self._locked_shard:
                raise ValueError("Can't count objects, no shard specified or locked")
            name = self._locked_shard[0]

        with self.pool.connection() as db:
            c = db.execute(
                """SELECT shard, COUNT(*)
                         FROM signature2shard
                         WHERE state = 'present'
                           AND shard = (SELECT id FROM shards WHERE name = %s)
                         GROUP BY shard
                """,
                (name,),
            )
            row = c.fetchone()
            if not row:
                return 0
            return row[1]

    def record_shard_mapped(self, host: str, name: str) -> Set[str]:
        with self.pool.connection() as db, db.transaction():
            with db.cursor() as c:
                c.execute(
                    """SELECT mapped_on_hosts_when_packed
                       FROM shards
                       WHERE name = %s
                       FOR UPDATE SKIP LOCKED""",
                    (name,),
                )
                row = c.fetchone()
                if not row:
                    raise ValueError("Can't update shard %s" % name)
                hosts = set(row[0])
                if host not in hosts:
                    hosts.add(host)
                    c.execute(
                        """UPDATE shards
                           SET mapped_on_hosts_when_packed = %s
                           WHERE name = %s""",
                        (list(hosts), name),
                    )
                return hosts

    def contains(self, obj_id) -> Optional[int]:
        with self.pool.connection() as db, db.cursor() as c:
            c.execute(
                "SELECT shard FROM signature2shard WHERE "
                "signature = %s AND state = 'present'",
                (obj_id,),
            )
            row = c.fetchone()
            if not row:
                return None
            return row[0]

    def get(self, obj_id) -> Optional[Tuple[str, ShardState]]:
        id = self.contains(obj_id)
        if id is None:
            return None
        return self.get_shard_info(id)

    def record_new_obj_id(self, db, obj_id) -> Optional[int]:
        db.execute(
            "INSERT INTO signature2shard (signature, shard, state) "
            "VALUES (%s, %s, 'present') ON CONFLICT (signature) DO NOTHING",
            (obj_id, self.locked_shard_id),
        )
        cur = db.execute(
            "SELECT shard FROM signature2shard WHERE signature = %s", (obj_id,)
        )
        return cur.fetchone()[0]

    def delete(self, obj_id):
        with self.pool.connection() as db:
            db.execute(
                "UPDATE signature2shard SET state = 'deleted' WHERE signature = %s",
                (obj_id,),
            )

    def deleted_objects(self) -> Iterator[Tuple[bytes, str, ShardState]]:
        with self.pool.connection() as db:
            cur = db.execute(
                """SELECT signature, shards.name, shards.state
               FROM signature2shard objs, shards
               WHERE objs.state = 'deleted'
                 AND shards.id = objs.shard
               """
            )
            for signature, name, state in cur.fetchall():
                yield bytes(signature), name, ShardState(state)

    def clean_deleted_object(self, obj_id) -> None:
        with self.pool.connection() as db:
            db.execute("DELETE FROM signature2shard WHERE signature = %s", (obj_id,))
