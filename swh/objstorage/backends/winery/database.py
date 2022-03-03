# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
from contextlib import contextmanager
import logging
import time

import psycopg2

logger = logging.getLogger(__name__)


class DatabaseAdmin:
    def __init__(self, dsn, dbname=None):
        self.dsn = dsn
        self.dbname = dbname

    @contextmanager
    def admin_cursor(self):
        db = psycopg2.connect(dsn=self.dsn, dbname="postgres")
        # https://wiki.postgresql.org/wiki/Psycopg2_Tutorial
        # If you want to drop the database you would need to
        # change the isolation level of the database.
        db.set_isolation_level(0)
        db.autocommit = True
        c = db.cursor()
        try:
            yield c
        finally:
            c.close()

    def create_database(self):
        with self.admin_cursor() as c:
            c.execute(
                "SELECT datname FROM pg_catalog.pg_database "
                f"WHERE datname = '{self.dbname}'"
            )
            if c.rowcount == 0:
                try:
                    c.execute(f"CREATE DATABASE {self.dbname}")
                except psycopg2.errors.UniqueViolation:
                    # someone else created the database, it is fine
                    pass

    def drop_database(self):
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
                    return
                except psycopg2.errors.ObjectInUse:
                    logger.warning(f"{self.dbname} database drop fails, waiting 10s")
                    time.sleep(10)
                    continue
            raise Exception(f"database drop failed on {self.dbname}")

    def list_databases(self):
        with self.admin_cursor() as c:
            c.execute(
                "SELECT datname FROM pg_database "
                "WHERE datistemplate = false and datname != 'postgres'"
            )
            return [r[0] for r in c.fetchall()]


class Database(abc.ABC):
    def __init__(self, dsn, dbname):
        self.dsn = dsn
        self.dbname = dbname

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

    def create_tables(self):
        db = psycopg2.connect(dsn=self.dsn, dbname=self.dbname)
        db.autocommit = True
        c = db.cursor()
        c.execute("SELECT pg_advisory_lock(%s)", (self.lock,))
        for table in self.database_tables:
            c.execute(table)
        c.close()
        db.close()  # so the pg_advisory_lock is released

    def connect_database(self):
        db = psycopg2.connect(dsn=self.dsn, dbname=self.dbname)
        db.autocommit = True
        return db
