# Copyright (C) 2021-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import logging
import os
from typing import Dict, Optional, Tuple

from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)


class PoolManager:
    """Manage a set of connection pools"""

    def __init__(self) -> None:
        self.pools: Dict[Tuple[str, Optional[str]], ConnectionPool] = {}
        self.refcounts: Dict[Tuple[str, Optional[str]], int] = {}

        os.register_at_fork(after_in_child=self.reset_state)

    def reset_state(self) -> None:
        """Clean up the state after forking, ConnectionPools aren't multiprocess-safe"""
        logger.debug("Fork detected, resetting PoolManager")
        self.pools.clear()
        self.refcounts.clear()

    def get(self, conninfo: str, application_name: Optional[str]) -> ConnectionPool:
        """Get a reference to this connection pool"""
        key = (conninfo, application_name)
        if key not in self.pools:
            logger.debug("Creating connection pool for app=%s", application_name)
            self.refcounts[key] = 0
            self.pools[key] = ConnectionPool(
                conninfo=conninfo,
                kwargs={
                    "application_name": application_name,
                    "fallback_application_name": "SWH Winery",
                    "autocommit": True,
                },
                name=f"pool-{application_name}" if application_name else "pool",
                min_size=0,
                max_size=4,
                open=True,
                max_idle=5,
                check=ConnectionPool.check_connection,
            )
            logger.debug("Connection pools managed: %s", len(self.pools))

        self.refcounts[key] += 1
        return self.pools[key]

    def release(self, conninfo: str, application_name: str) -> None:
        """Release a reference to this connection pool"""
        key = (conninfo, application_name)
        if key not in self.pools:
            return

        self.refcounts[key] -= 1
        if self.refcounts[key] <= 0:
            logger.debug("Closing pool for app=%s", application_name)
            del self.refcounts[key]
            self.pools[key].close()
            del self.pools[key]
            logger.debug("Connection pools managed: %s", len(self.pools))


POOLS = PoolManager()


class Database(abc.ABC):
    def __init__(self, dsn, application_name=None):
        self.dsn = dsn
        self.application_name = application_name
        self._pool = None

    @property
    def pool(self):
        if not self._pool:
            self._pool = POOLS.get(
                conninfo=self.dsn,
                application_name=self.application_name,
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

    def uninit(self):
        if self._pool:
            self._pool = None
            POOLS.release(
                conninfo=self.dsn,
                application_name=self.application_name,
            )

    def __del__(self):
        # Release the connection pool
        self.uninit()
