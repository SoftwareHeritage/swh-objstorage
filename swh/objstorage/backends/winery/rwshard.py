# Copyright (C) 2021-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from contextlib import contextmanager, nullcontext
from functools import partial
import logging
from threading import Event, Thread
import time
from typing import Callable, ContextManager, Iterator, Optional, Protocol, Tuple

import psycopg

from .database import Database

logger = logging.getLogger(__name__)


class IdleHandler(Thread):
    """Call the `callback` after being idle for `timeout` seconds."""

    def __init__(self, name: str, timeout: float, callback: Callable[[], None]):
        super().__init__(name=f"IdleHandler-{name}")
        self.timeout = timeout
        self.callback = callback
        self.deadline = time.monotonic() + timeout
        self.quiesced = Event()
        self.terminated = Event()

    def quiesce(self):
        """Quiesce the timeout.

        This should generally be used via the :func:`quiesce_then_reset` context
        manager, which wraps a block of code to quiesce the timeout while the
        code runs, then resets the timeout on completion.

        """
        self.quiesced.set()

    def reset(self):
        """Reset the timeout clock.

        This should generally be used via the :func:`quiesce_then_reset` context
        manager, which wraps a block of code to quiesce the timeout while the
        code runs, then resets the timeout on completion.
        """
        self.deadline = time.monotonic() + self.timeout
        self.quiesced.clear()

    @contextmanager
    def quiesce_then_reset(self):
        """Wrap a block of code to quiesce the timeout while the code runs,
        then reset the timeout on completion.
        """
        self.quiesce()
        yield
        self.reset()

    def join(self, timeout=None):
        """Gracefully terminate the thread."""
        self.terminated.set()
        # Trigger exit from the main loop by setting the quiesced event
        self.quiesced.set()
        return super().join(timeout)

    def run(self):
        while True:
            # Wait at least 1 second when paused
            wait_for = max(self.deadline - time.monotonic(), 1)
            quiesced = self.quiesced.wait(timeout=wait_for)
            if self.terminated.is_set():
                break
            if quiesced:
                time.sleep(0.1)
                continue
            if time.monotonic() > self.deadline:
                break

        if not self.terminated.is_set():
            logger.debug("Idle timeout reached, calling idle callback")
            self.callback()


class ShardIdleTimeoutCallback(Protocol):
    """A function which takes a :class:`RWShard` as `shard` argument, used as
    idle timeout callback for :class:`RWShard`."""

    def __call__(self, shard: "RWShard") -> None:
        ...


class RWShard(Database):
    def __init__(
        self,
        name: str,
        base_dsn: str,
        shard_max_size: int,
        application_name: Optional[str] = None,
        idle_timeout_cb: Optional[ShardIdleTimeoutCallback] = None,
        idle_timeout: Optional[float] = 5,
        **kwargs,
    ):
        self._name = name
        if application_name is None:
            application_name = f"SWH Winery RW Shard {name}"
        super().__init__(dsn=base_dsn, application_name=application_name)
        self.create()
        self.size = self.total_size()
        self.limit = shard_max_size

        self.idle_handler: Optional[IdleHandler] = None
        self.quiesce_then_reset_idle: Callable[[], ContextManager] = nullcontext

        if idle_timeout and idle_timeout_cb:
            self.idle_handler = IdleHandler(
                name=name,
                timeout=idle_timeout,
                callback=partial(idle_timeout_cb, shard=self),
            )
            self.idle_handler.start()
            self.quiesce_then_reset_idle = self.idle_handler.quiesce_then_reset

    def disable_idle_handler(self):
        if thread := getattr(self, "idle_handler"):
            thread.join()
            self.idle_handler = None
            self.quiesce_then_reset_idle = nullcontext

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
                "(LIKE shard_template INCLUDING ALL) "
                "WITH (autovacuum_enabled = false)"
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

    def add(self, db: psycopg.Connection, obj_id: bytes, content: bytes) -> None:
        with self.quiesce_then_reset_idle():
            cur = db.execute(
                f"INSERT INTO {self.table_name} (key, content) "
                "VALUES (%s, %s) "
                "ON CONFLICT (key) DO NOTHING",
                (obj_id, content),
                binary=True,
            )
            if cur.rowcount:
                self.size += len(content)

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
