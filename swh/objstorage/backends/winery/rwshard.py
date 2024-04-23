# Copyright (C) 2021-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import logging
from typing import Iterator, Optional, Tuple

import psycopg
import psycopg.errors

from .database import Database

logger = logging.getLogger(__name__)


class RWShard(Database):
    def __init__(self, name: str, application_name: Optional[str] = None, **kwargs):
        self._name = name
        self.application_name = application_name
        if application_name is None:
            self.application_name = f"SWH Winery RW Shard {name}"
        super().__init__(kwargs["base_dsn"], self.application_name)
        self.create()
        self.size = self.total_size()
        self.limit = kwargs["shard_max_size"]

    @property
    def name(self) -> str:
        return self._name

    @property
    def table_name(self) -> str:
        return f"shard_{self._name}"

    def is_full(self) -> bool:
        return self.size >= self.limit

    def create(self) -> None:
        with self.pool.connection() as db:
            db.execute(
                f"CREATE TABLE IF NOT EXISTS {self.table_name} "
                "(LIKE shard_template INCLUDING ALL)"
            )

    def drop(self) -> None:
        with self.pool.connection() as db:
            db.execute(f"DROP TABLE {self.table_name}")

    def total_size(self) -> int:
        with self.pool.connection() as db, db.cursor() as c:
            c.execute(f"SELECT SUM(LENGTH(content)) FROM {self.table_name}")
            size = c.fetchone()[0]
            if size is None:
                return 0
            else:
                return size

    def add(self, obj_id: bytes, content: bytes) -> None:
        try:
            with self.pool.connection() as db, db.cursor() as c:
                c.execute(
                    f"INSERT INTO {self.table_name} (key, content) VALUES (%s, %s)",
                    (obj_id, content),
                    binary=True,
                )
            self.size += len(content)
        except psycopg.errors.UniqueViolation:
            pass

    def get(self, obj_id: bytes) -> Optional[bytes]:
        with self.pool.connection() as db, db.cursor() as c:
            c.execute(
                f"SELECT content FROM {self.table_name} WHERE key = %s",
                (obj_id,),
                binary=True,
            )
            if c.rowcount == 0:
                return None
            else:
                return c.fetchone()[0]

    def delete(self, obj_id: bytes) -> None:
        with self.pool.connection() as db, db.cursor() as c:
            c.execute(f"DELETE FROM {self.table_name} WHERE key = %s", (obj_id,))
            if c.rowcount == 0:
                raise KeyError(obj_id)

    def all(self) -> Iterator[Tuple[bytes, bytes]]:
        with self.pool.connection() as db, db.cursor() as c:
            with c.copy(
                f"COPY {self.table_name} (key, content) TO STDOUT (FORMAT BINARY)"
            ) as copy:
                copy.set_types(["bytea", "bytea"])
                yield from copy.rows()

    def count(self) -> int:
        with self.pool.connection() as db, db.cursor() as c:
            c.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            return c.fetchone()[0]
