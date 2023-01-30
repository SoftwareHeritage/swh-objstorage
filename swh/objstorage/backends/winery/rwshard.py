# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import psycopg2

from .database import Database, DatabaseAdmin


class RWShard(Database):
    def __init__(self, name, **kwargs):
        self._name = name
        DatabaseAdmin(kwargs["base_dsn"], self.name).create_database()
        super().__init__(kwargs["shard_dsn"], self.name)
        self.create_tables()
        self.db = self.connect_database()
        self.size = self.total_size()
        self.limit = kwargs["shard_max_size"]

    def uninit(self):
        if hasattr(self, "db"):
            self.db.close()
            del self.db

    @property
    def lock(self):
        return 452343  # an arbitrary unique number

    @property
    def name(self):
        return self._name

    def is_full(self):
        return self.size > self.limit

    def drop(self):
        DatabaseAdmin(self.dsn, self.dbname).drop_database()

    @property
    def database_tables(self):
        return [
            """
        CREATE TABLE IF NOT EXISTS objects(
          key BYTEA PRIMARY KEY,
          content BYTEA
        )
        """,
        ]

    def total_size(self):
        with self.db.cursor() as c:
            c.execute("SELECT SUM(LENGTH(content)) FROM objects")
            size = c.fetchone()[0]
            if size is None:
                return 0
            else:
                return size

    def add(self, obj_id, content):
        try:
            with self.db.cursor() as c:
                c.execute(
                    "INSERT INTO objects (key, content) VALUES (%s, %s)",
                    (obj_id, content),
                )
            self.db.commit()
            self.size += len(content)
        except psycopg2.errors.UniqueViolation:
            pass

    def get(self, obj_id):
        with self.db.cursor() as c:
            c.execute("SELECT content FROM objects WHERE key = %s", (obj_id,))
            if c.rowcount == 0:
                return None
            else:
                return c.fetchone()[0].tobytes()

    def all(self):
        with self.db.cursor() as c:
            c.execute("SELECT key,content FROM objects")
            for row in c:
                yield row[0].tobytes(), row[1].tobytes()

    def count(self):
        with self.db.cursor() as c:
            c.execute("SELECT COUNT(*) FROM objects")
            return c.fetchone()[0]
