# Copyright (C) 2021-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
from contextlib import contextmanager
import logging
import os
import time
from typing import Dict, Optional, Set, Tuple

import psycopg
import psycopg.errors
from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)

DATABASES_CREATED: Set[Tuple[str, str]] = set()
"""Set of (conninfo, dbname) entries for databases that we know have been created"""

TABLES_CREATED: Set[Tuple[str, str]] = set()
"""Set of (conninfo, dbname) entries for databases for which we know tables have been created"""


class PoolManager:
    """Manage a set of connection pools"""

    def __init__(self) -> None:
        self.pools: Dict[Tuple[str, str, Optional[str]], ConnectionPool] = {}
        self.refcounts: Dict[Tuple[str, str, Optional[str]], int] = {}

        os.register_at_fork(after_in_child=self.reset_state)

    def reset_state(self) -> None:
        """Clean up the state after forking, ConnectionPools aren't multiprocess-safe"""
        logger.debug("Fork detected, resetting PoolManager")
        self.pools.clear()
        self.refcounts.clear()

    def get(
        self, conninfo: str, dbname: str, application_name: Optional[str]
    ) -> ConnectionPool:
        """Get a reference to this connection pool"""
        key = (conninfo, dbname, application_name)
        if key not in self.pools:
            logger.debug(
                "Creating connection pool for %s, app=%s", dbname, application_name
            )
            self.refcounts[key] = 0
            self.pools[key] = ConnectionPool(
                conninfo=conninfo,
                kwargs={
                    "dbname": dbname,
                    "application_name": application_name,
                    "fallback_application_name": "SWH Winery",
                    "autocommit": True,
                },
                name=(
                    f"pool-{dbname}"
                    + (f"-{application_name}" if application_name else "")
                ),
                min_size=0,
                max_size=4,
                open=True,
                max_idle=5,
                check=ConnectionPool.check_connection,
            )
            logger.debug("Connection pools managed: %s", len(self.pools))

        self.refcounts[key] += 1
        return self.pools[key]

    def release(self, conninfo: str, dbname: str, application_name: str) -> None:
        """Release a reference to this connection pool"""
        key = (conninfo, dbname, application_name)
        if key not in self.pools:
            return

        self.refcounts[key] -= 1
        if self.refcounts[key] <= 0:
            logger.debug("Closing pool for %s, app=%s", dbname, application_name)
            del self.refcounts[key]
            self.pools[key].close()
            del self.pools[key]
            logger.debug("Connection pools managed: %s", len(self.pools))


POOLS = PoolManager()


class DatabaseAdmin:
    def __init__(self, dsn, dbname=None, application_name=None):
        self.dsn = dsn
        self.dbname = dbname
        self.application_name = application_name

    @contextmanager
    def admin_cursor(self):
        db = psycopg.connect(
            conninfo=self.dsn,
            dbname="postgres",
            autocommit=True,
            application_name=self.application_name,
            fallback_application_name="SWH Winery Admin",
        )
        c = db.cursor()
        try:
            yield c
        finally:
            c.close()

    def create_database(self):
        if (self.dsn, self.dbname) in DATABASES_CREATED:
            return

        logger.debug("database %s: create", self.dbname)
        with self.admin_cursor() as c:
            c.execute(
                "SELECT datname FROM pg_catalog.pg_database "
                f"WHERE datname = '{self.dbname}'"
            )
            if c.rowcount == 0:
                try:
                    c.execute(f"CREATE DATABASE {self.dbname}")
                except (
                    psycopg.errors.UniqueViolation,
                    psycopg.errors.DuplicateDatabase,
                ):
                    # someone else created the database, it is fine
                    pass

        DATABASES_CREATED.add((self.dsn, self.dbname))

    def drop_database(self):
        logger.debug("database %s/%s: drop", self.dsn, self.dbname)
        with self.admin_cursor() as c:
            c.execute(
                "SELECT pg_terminate_backend(pg_stat_activity.pid)"
                "FROM pg_stat_activity "
                "WHERE pg_stat_activity.datname = %s;",
                (self.dbname,),
            )
            #
            # Dropping the database may fail because the server takes time
            # to notice a connection was dropped and/or a named cursor is
            # in the process of being deleted. It can happen here or even
            # when deleting all database with the psql cli
            # and there are no process active.
            #
            # ERROR: database "i606428a5a6274d1ab09eecc4d019fef7" is being
            # accessed by other users DETAIL: There is 1 other session
            # using the database.
            #
            # See:
            # https://stackoverflow.com/questions/5108876/kill-a-postgresql-session-connection
            #
            # https://www.postgresql.org/docs/current/sql-dropdatabase.html
            #
            # WITH (FORCE) added in postgresql 13 but may also fail because the
            # named cursor may not be handled as a client.
            #
            for i in range(60):
                try:
                    c.execute(f"DROP DATABASE IF EXISTS {self.dbname}")
                    break
                except psycopg.errors.ObjectInUse:
                    logger.warning(f"{self.dbname} database drop fails, waiting 10s")
                    time.sleep(10)
                    continue
            else:
                raise Exception(f"database drop failed on {self.dbname}")

        DATABASES_CREATED.discard((self.dsn, self.dbname))
        TABLES_CREATED.discard((self.dsn, self.dbname))

    def list_databases(self):
        with self.admin_cursor() as c:
            c.execute(
                "SELECT datname FROM pg_database "
                "WHERE datistemplate = false and datname != 'postgres'"
            )
            return [r[0] for r in c.fetchall()]


class Database(abc.ABC):
    def __init__(self, dsn, dbname, application_name=None):
        self.dsn = dsn
        self.dbname = dbname
        self.application_name = application_name
        self._pool = None

    @property
    def pool(self):
        if not self._pool:
            self._pool = POOLS.get(
                conninfo=self.dsn,
                dbname=self.dbname,
                application_name=self.application_name,
            )
        return self._pool

    @property
    @abc.abstractmethod
    def lock(self):
        "Return an arbitrary unique number for pg_advisory_lock when creating tables"
        raise NotImplementedError("Database.lock")

    @property
    @abc.abstractmethod
    def database_tables(self):
        "Return the list of CREATE TABLE statements for all tables in the database"
        raise NotImplementedError("Database.database_tables")

    def uninit(self):
        if self._pool:
            self._pool = None
            POOLS.release(
                conninfo=self.dsn,
                dbname=self.dbname,
                application_name=self.application_name,
            )

    def __del__(self):
        # Release the connection pool
        self.uninit()

    def create_tables(self):
        if (self.dsn, self.dbname) in TABLES_CREATED:
            return

        logger.debug("database %s: create tables", self.dbname)
        with self.pool.connection() as db:
            db.execute("SELECT pg_advisory_lock(%s)", (self.lock,))
            for table in self.database_tables:
                db.execute(table)
            db.execute("SELECT pg_advisory_unlock(%s)", (self.lock,))

        TABLES_CREATED.add((self.dsn, self.dbname))
