# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import uuid

import psycopg2

from .database import Database, DatabaseAdmin


class SharedBase(Database):
    def __init__(self, **kwargs):
        DatabaseAdmin(kwargs["base_dsn"], "sharedbase").create_database()
        super().__init__(kwargs["base_dsn"], "sharedbase")
        self.create_tables()
        self.db = self.connect_database()
        self._whoami = None

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

        while True:
            self._whoami, self._whoami_id = self.lock_a_shard()
            if self._whoami is not None:
                return self._whoami
            self.create_shard()

    def lock_a_shard(self):
        with self.db.cursor() as c:
            c.execute(
                "SELECT name FROM shards WHERE readonly = FALSE and packing = FALSE "
                "LIMIT 1 FOR UPDATE SKIP LOCKED"
            )
            if c.rowcount == 0:
                return None, None
            name = c.fetchone()[0]
        return self.lock_shard(name)

    def lock_shard(self, name):
        self.whoami_lock = self.db.cursor()
        try:
            self.whoami_lock.execute(
                "SELECT name, id FROM shards "
                "WHERE readonly = FALSE AND packing = FALSE AND name = %s "
                "FOR UPDATE NOWAIT",
                (name,),
            )
            return self.whoami_lock.fetchone()
        except psycopg2.Error:
            return None

    def unlock_shard(self):
        del self.whoami_lock

    def create_shard(self):
        name = uuid.uuid4().hex
        #
        # ensure the first character is not a number so it can be used as a
        # database name.
        #
        name = "i" + name[1:]
        with self.db.cursor() as c:
            c.execute(
                "INSERT INTO shards (name, readonly, packing) "
                "VALUES (%s, FALSE, FALSE)",
                (name,),
            )
        self.db.commit()

    def shard_packing_starts(self):
        with self.db.cursor() as c:
            c.execute(
                "UPDATE shards SET packing = TRUE WHERE name = %s", (self.whoami,)
            )
        self.unlock_shard()

    def shard_packing_ends(self, name):
        with self.db.cursor() as c:
            c.execute(
                "UPDATE shards SET readonly = TRUE, packing = FALSE " "WHERE name = %s",
                (name,),
            )

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
