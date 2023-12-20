# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from enum import Enum
import logging
from typing import Iterator, Optional, Set, Tuple
import uuid

import psycopg2
import psycopg2.errors

from .database import Database, DatabaseAdmin

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


class SharedBase(Database):
    def __init__(self, **kwargs) -> None:
        DatabaseAdmin(kwargs["base_dsn"], "sharedbase").create_database()
        super().__init__(kwargs["base_dsn"], "sharedbase")
        self.create_tables()
        self.db = self.connect_database()
        self._locked_shard: Optional[Tuple[str, int]] = None

        logger.debug("SharedBase %s: instantiated", WRITER_UUID)

    def uninit(self):
        if self._locked_shard is not None:
            self.set_shard_state(new_state=ShardState.STANDBY)
        self._locked_shard = None

    @property
    def lock(self):
        return 314116  # an arbitrary unique number

    @property
    def database_tables(self):
        return [
            f"""\
        DO $$ BEGIN
          CREATE TYPE shard_state AS ENUM (
            {", ".join("'%s'" % value.value for value in ShardState)}
          );
        EXCEPTION
          WHEN duplicate_object THEN null;
        END $$;
            """,
            "ALTER TYPE shard_state ADD VALUE IF NOT EXISTS 'cleaning' AFTER 'packed';",
            """
        CREATE TABLE IF NOT EXISTS shards(
            id BIGSERIAL PRIMARY KEY,
            state shard_state NOT NULL DEFAULT 'standby',
            locker_ts TIMESTAMPTZ,
            locker UUID,
            name CHAR(32) NOT NULL UNIQUE,
            mapped_on_hosts_when_packed TEXT[] NOT NULL DEFAULT '{}'
        )
        """,
            """\
        ALTER TABLE shards
            ADD COLUMN
              IF NOT EXISTS
              mapped_on_hosts_when_packed TEXT[]
              NOT NULL
              DEFAULT '{}'
        """,
            """
        CREATE TABLE IF NOT EXISTS signature2shard(
            signature BYTEA PRIMARY KEY,
            inflight BOOLEAN NOT NULL,
            shard BIGINT NOT NULL REFERENCES shards(id)
        )
        """,
        ]

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

        with self.db:
            # run the next two statements in a transaction
            with self.db.cursor() as c:
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
                except psycopg2.Error:
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

    def set_shard_state(
        self,
        new_state: ShardState,
        set_locker: bool = False,
        check_locker: bool = False,
        name: Optional[str] = None,
    ):
        reset_locked_shard = False
        if not name:
            if not self._locked_shard:
                raise ValueError("Can't set shard state, no shard specified or locked")
            name = self._locked_shard[0]
            reset_locked_shard = True

        with self.db.cursor() as c:
            c.execute(
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
        with self.db.cursor() as c:
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
        with self.db:
            with self.db.cursor() as c:
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
                )

    def shard_packing_ends(self, name):
        with self.db:
            with self.db.cursor() as c:
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
                )

    def get_shard_info(self, id: int) -> Optional[Tuple[str, ShardState]]:
        with self.db.cursor() as c:
            c.execute("SELECT name, state FROM shards WHERE id = %s", (id,))
            row = c.fetchone()
            if not row:
                return None
            return (row[0], ShardState(row[1]))

    def get_shard_state(self, name: str) -> Optional[ShardState]:
        with self.db.cursor() as c:
            c.execute("SELECT state FROM shards WHERE name = %s", (name,))
            row = c.fetchone()
            if not row:
                return None
            return ShardState(row[0])

    def list_shards(self) -> Iterator[Tuple[str, ShardState]]:
        with self.db.cursor() as c:
            c.execute("SELECT name, state FROM shards")
            for row in c:
                yield row[0], ShardState(row[1])

    def record_shard_mapped(self, host: str, name: Optional[str] = None) -> Set[str]:
        if not name:
            if not self._locked_shard:
                raise ValueError("Can't set shard state, no shard specified or locked")
            name = self._locked_shard[0]

        with self.db:
            with self.db.cursor() as c:
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
        with self.db.cursor() as c:
            c.execute(
                "SELECT shard FROM signature2shard WHERE "
                "signature = %s AND inflight = FALSE",
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

    def add_phase_1(self, obj_id) -> Optional[int]:
        try:
            with self.db.cursor() as c:
                c.execute(
                    "INSERT INTO signature2shard (signature, shard, inflight) "
                    "VALUES (%s, %s, TRUE)",
                    (obj_id, self.locked_shard_id),
                )
            self.db.commit()
            return self.locked_shard_id
        except psycopg2.errors.UniqueViolation:
            with self.db.cursor() as c:
                c.execute(
                    "SELECT shard FROM signature2shard WHERE "
                    "signature = %s AND inflight = TRUE",
                    (obj_id,),
                )
                row = c.fetchone()
                if not row:
                    return None
                return row[0]

    def add_phase_2(self, obj_id):
        with self.db.cursor() as c:
            c.execute(
                "UPDATE signature2shard SET inflight = FALSE "
                "WHERE signature = %s AND shard = %s",
                (obj_id, self.locked_shard_id),
            )
        self.db.commit()
