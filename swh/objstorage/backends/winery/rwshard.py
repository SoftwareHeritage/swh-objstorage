# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import logging

import psycopg
import psycopg.errors

from .database import Database, DatabaseAdmin

logger = logging.getLogger(__name__)


class RWShard(Database):
    def __init__(self, name, application_name=None, **kwargs):
        self._name = name
        self.application_name = application_name
        if application_name is None:
            self.application_name = f"SWH Winery RW Shard {name}"
        DatabaseAdmin(
            kwargs["base_dsn"],
            self.name,
            application_name=f"Admin {self.application_name}",
        ).create_database()
        super().__init__(kwargs["shard_dsn"], self.name, self.application_name)
        self.create_tables()
        self.size = self.total_size()
        self.limit = kwargs["shard_max_size"]

    @property
    def lock(self):
        return 452343  # an arbitrary unique number

    @property
    def name(self):
        return self._name

    def is_full(self):
        return self.size >= self.limit

    def drop(self):
        DatabaseAdmin(self.dsn, self.dbname).drop_database()

    @property
    def database_tables(self):
        return [
            """
        CREATE TABLE IF NOT EXISTS objects(
          key BYTEA PRIMARY KEY,
          content BYTEA
        ) WITH (autovacuum_enabled = false)
        """,
        ]

    def total_size(self):
        with self.pool.connection() as db, db.cursor() as c:
            c.execute("SELECT SUM(LENGTH(content)) FROM objects")
            size = c.fetchone()[0]
            if size is None:
                return 0
            else:
                return size

    def add(self, obj_id, content):
        try:
            with self.pool.connection() as db, db.cursor() as c:
                c.execute(
                    "INSERT INTO objects (key, content) VALUES (%s, %s)",
                    (obj_id, content),
                    binary=True,
                )
            self.size += len(content)
        except psycopg.errors.UniqueViolation:
            pass

    def get(self, obj_id):
        with self.pool.connection() as db, db.cursor() as c:
            c.execute(
                "SELECT content FROM objects WHERE key = %s", (obj_id,), binary=True
            )
            if c.rowcount == 0:
                return None
            else:
                return c.fetchone()[0]

    def delete(self, obj_id):
        with self.pool.connection() as db, db.cursor() as c:
            c.execute("DELETE FROM objects WHERE key = %s", (obj_id,))
            if c.rowcount == 0:
                raise KeyError(obj_id)

    def all(self):
        with self.pool.connection() as db, db.cursor() as c:
            with c.copy(
                "COPY objects (key, content) TO STDOUT (FORMAT BINARY)"
            ) as copy:
                copy.set_types(["bytea", "bytea"])
                yield from copy.rows()

    def count(self):
        with self.pool.connection() as db, db.cursor() as c:
            c.execute("SELECT COUNT(*) FROM objects")
            return c.fetchone()[0]
