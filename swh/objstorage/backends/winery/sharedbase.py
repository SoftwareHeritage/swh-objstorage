# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from typing import Optional, Tuple
import uuid

import psycopg2

from .database import Database, DatabaseAdmin

WRITER_UUID = uuid.uuid4()

logger = logging.getLogger(__name__)


class SharedBase(Database):
    def __init__(self, **kwargs):
        DatabaseAdmin(kwargs["base_dsn"], "sharedbase").create_database()
        super().__init__(kwargs["base_dsn"], "sharedbase")
        self.create_tables()
        self.db = self.connect_database()
        self._whoami: str = None
        self._whoami_id: int = None

        logger.debug("SharedBase %s: instantiated", WRITER_UUID)

    def uninit(self):
        self.db.close()
        del self.db

    @property
    def lock(self):
        return 314116  # an arbitrary unique number

    @property
    def database_tables(self):
        return [
            """
        CREATE TABLE IF NOT EXISTS shards(
            id SERIAL PRIMARY KEY,
            readonly BOOLEAN NOT NULL,
            packing BOOLEAN NOT NULL,
            active_writer_ts TIMESTAMPTZ,
            active_writer UUID,
            name CHAR(32) NOT NULL UNIQUE
        )
        """,
            """
        CREATE TABLE IF NOT EXISTS signature2shard(
            signature BYTEA PRIMARY KEY,
            inflight BOOLEAN NOT NULL,
            shard INTEGER NOT NULL
        )
        """,
        ]

    @property
    def whoami(self):
        self.set_whoami()
        return self._whoami

    @property
    def id(self):
        self.set_whoami()
        return self._whoami_id

    def set_whoami(self):
        if self._whoami is not None:
            return

        locked = self.lock_shard()
        if locked:
            self._whoami, self._whoami_id = locked
        else:
            self._whoami, self._whoami_id = self.create_shard()

        return self._whoami

    def lock_shard(self) -> Optional[Tuple[str, int]]:
        with self.db:
            # run the next two statements in a transaction
            with self.db.cursor() as c:
                c.execute(
                    """\
                    SELECT name
                    FROM shards
                    WHERE readonly = FALSE and packing = FALSE and active_writer IS NULL
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    """
                )
                result = c.fetchone()
                if result is None:
                    return None
                shard_name = result[0]
                try:
                    c.execute(
                        """\
                        UPDATE shards
                        SET active_writer_ts = now(), active_writer = %s
                        WHERE
                            name = %s
                            AND readonly = FALSE
                            AND packing = FALSE
                            AND active_writer IS NULL
                        RETURNING name, id
                        """,
                        (WRITER_UUID, shard_name),
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

    def unlock_shard(self):
        with self.db.cursor() as c:
            c.execute(
                """\
                UPDATE shards
                SET active_writer = NULL
                WHERE name = %s AND active_writer = %s
                """,
                (self._whoami, WRITER_UUID),
            )
            logger.debug("SharedBase %s: shard %s unlocked", WRITER_UUID, self._whoami)

    def create_shard(self) -> Tuple[str, int]:
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
                  (name, readonly, packing, active_writer, active_writer_ts)
                VALUES
                  (%s, FALSE, FALSE, %s, NOW())
                RETURNING name, id""",
                (name, WRITER_UUID),
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

    def shard_packing_starts(self):
        with self.db.cursor() as c:
            c.execute(
                "UPDATE shards SET packing = TRUE WHERE name = %s", (self.whoami,)
            )
        logger.debug("SharedBase %s: shard %s starts packing", WRITER_UUID, self.whoami)
        self.unlock_shard()

    def shard_packing_ends(self, name):
        with self.db.cursor() as c:
            c.execute(
                "UPDATE shards SET readonly = TRUE, packing = FALSE " "WHERE name = %s",
                (name,),
            )
        logger.debug("SharedBase %s: shard %s ends packing", WRITER_UUID, name)

    def get_shard_info(self, id):
        with self.db.cursor() as c:
            c.execute("SELECT name, readonly FROM shards WHERE id = %s", (id,))
            if c.rowcount == 0:
                return None
            else:
                return c.fetchone()

    def list_shards(self):
        with self.db.cursor() as c:
            c.execute("SELECT name, readonly, packing FROM shards")
            for row in c:
                yield row[0], row[1], row[2]

    def contains(self, obj_id):
        with self.db.cursor() as c:
            c.execute(
                "SELECT shard FROM signature2shard WHERE "
                "signature = %s AND inflight = FALSE",
                (obj_id,),
            )
            if c.rowcount == 0:
                return None
            else:
                return c.fetchone()[0]

    def get(self, obj_id):
        id = self.contains(obj_id)
        if id is None:
            return None
        return self.get_shard_info(id)

    def add_phase_1(self, obj_id):
        try:
            with self.db.cursor() as c:
                c.execute(
                    "INSERT INTO signature2shard (signature, shard, inflight) "
                    "VALUES (%s, %s, TRUE)",
                    (obj_id, self.id),
                )
            self.db.commit()
            return self.id
        except psycopg2.errors.UniqueViolation:
            with self.db.cursor() as c:
                c.execute(
                    "SELECT shard FROM signature2shard WHERE "
                    "signature = %s AND inflight = TRUE",
                    (obj_id,),
                )
                if c.rowcount == 0:
                    return None
                else:
                    return c.fetchone()[0]

    def add_phase_2(self, obj_id):
        with self.db.cursor() as c:
            c.execute(
                "UPDATE signature2shard SET inflight = FALSE "
                "WHERE signature = %s AND shard = %s",
                (obj_id, self.id),
            )
        self.db.commit()
