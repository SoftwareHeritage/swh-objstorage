# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import psycopg2

from .database import Database


class RWShard(Database):
    def __init__(self, name, **kwargs):
        super().__init__(kwargs["shard_dsn"])
        self._name = name
        self.create_database(self.name)
        self.db = self.create_table(f"{self.dsn}/{self.name}")
        self.size = self.total_size()
        self.limit = kwargs["shard_max_size"]

    def uninit(self):
        if hasattr(self, "db"):
            self.db.close()
            del self.db

    @property
    def name(self):
        return self._name

    def is_full(self):
        return self.size > self.limit

    def drop(self):
        self.drop_database(self.name)

    def create_table(self, dsn):
        db = psycopg2.connect(dsn)
        db.autocommit = True
        c = db.cursor()
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS objects(
          key BYTEA PRIMARY KEY,
          content BYTEA
        )
        """
        )
        c.close()
        return db

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
