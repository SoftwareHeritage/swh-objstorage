# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
from contextlib import contextmanager
import logging
import os
import time
from typing import Dict, Set, Tuple

import psycopg
import psycopg.errors
from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)

POOLS: Dict[Tuple[int, str, str, str], ConnectionPool] = {}
"""Maps a tuple (pid, conninfo, dbname, application_name) to the matching ConnectionPool"""

DATABASES_CREATED: Set[Tuple[str, str]] = set()
"""Set of (conninfo, dbname) entries for databases that we know have been created"""

TABLES_CREATED: Set[Tuple[str, str]] = set()
"""Set of (conninfo, dbname) entries for databases for which we know tables have been created"""


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
        pool_key = (os.getpid(), self.dsn, self.dbname, self.application_name)
        if pool_key not in POOLS:
            POOLS[pool_key] = ConnectionPool(
                conninfo=self.dsn,
                kwargs={
                    "dbname": self.dbname,
                    "application_name": self.application_name,
                    "fallback_application_name": "SWH Winery",
                    "autocommit": True,
                },
                min_size=0,
                max_size=4,
                open=True,
                max_idle=5,
                check=ConnectionPool.check_connection,
            )

        self.pool = POOLS[pool_key]

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
        pass

    def create_tables(self):
        if (self.dsn, self.dbname) in TABLES_CREATED:
            return

        logger.debug("database %s: create tables", self.dbname)
        logger.debug("pool stats: %s", self.pool.get_stats())
        with self.pool.connection() as db:
            db.execute("SELECT pg_advisory_lock(%s)", (self.lock,))
            for table in self.database_tables:
                db.execute(table)
            db.execute("SELECT pg_advisory_unlock(%s)", (self.lock,))

        TABLES_CREATED.add((self.dsn, self.dbname))
