# Copyright (C) 2021-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import logging

from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)


class Database(abc.ABC):
    def __init__(self, dsn: str, application_name: str):
        self.dsn = dsn
        self.application_name = application_name
        self._pool = None

    @property
    def pool(self):
        if not self._pool:
            self._pool = ConnectionPool(
                conninfo=self.dsn,
                kwargs={
                    "application_name": self.application_name,
                    "fallback_application_name": "SWH Winery",
                    "autocommit": True,
                },
                name=(
                    f"pool-{self.application_name}" if self.application_name else "pool"
                ),
                min_size=0,
                max_size=4,
                open=True,
                max_idle=5,
                check=ConnectionPool.check_connection,
            )
        return self._pool

    def list_shard_tables(self):
        QUERY = """
        SELECT c.relname
        FROM pg_catalog.pg_class c
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
        WHERE c.relkind = 'r'
              AND n.nspname <> 'pg_catalog'
              AND n.nspname !~ '^pg_toast'
              AND n.nspname <> 'information_schema'
              AND pg_catalog.pg_table_is_visible(c.oid)
              AND c.relname ~ '^shard_'
              AND c.relname <> 'shard_template'
        """
        with self.pool.connection() as db:
            c = db.execute(QUERY)
            return [r[0].removeprefix("shard_") for r in c]

    def __del__(self):
        # Release the connection pool
        if self._pool:
            self._pool.close()
            self._pool = None
