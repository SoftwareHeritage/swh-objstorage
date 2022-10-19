# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import datetime
import logging
import time

from .database import Database, DatabaseAdmin

logger = logging.getLogger(__name__)


class LeakyBucket:
    """
    Leaky bucket that can contain at most `total` and leaks it within a second.
    If adding (with the add method) more than `total` per second, it will sleep
    until the bucket can be filled without overflowing.

    The capacity of the bucket can be changed dynamically with the reset method.
    If the new capacity is lower than it previously was, the overflow is ignored.
    """

    def __init__(self, total):
        self.updated = 0.0
        self.current = 0.0
        self.reset(total)

    def reset(self, total):
        self.total = total
        self.current = min(self.total, self.current)
        self._tick()

    def add(self, count):
        self._tick()
        if count > self.current:
            time.sleep((count - self.current) / self.total)
            self._tick()
        self.current -= min(self.total, count)

    def _tick(self):
        now = time.monotonic()
        self.current += self.total * (now - self.updated)
        self.current = int(min(self.total, self.current))
        self.updated = now


class BandwidthCalculator:
    """Keeps a histogram (of length `duration`, defaults to 60) where
    each element is the number of bytes read or written within a
    second.

    Only the last `duration` seconds are represented in the histogram:
    after each second the oldest element is discarded.

    The `add` method is called to add a value to the current second.

    The `get` method retrieves the current bandwidth usage which is
    the average of all values in the histogram.
    """

    def __init__(self):
        self.duration = 60
        self.history = collections.deque([0] * (self.duration - 1), self.duration - 1)
        self.current = 0
        self.current_second = 0

    def add(self, count):
        current_second = int(time.monotonic())
        if current_second > self.current_second:
            self.history.append(self.current)
            self.history.extend(
                [0] * min(self.duration, current_second - self.current_second - 1)
            )
            self.current_second = current_second
            self.current = 0
        self.current += count

    def get(self):
        return (sum(self.history) + self.current) / self.duration


class IOThrottler(Database):
    """Throttle IO (either read or write, depending on the `name`
    argument). The maximum speed in bytes is from the throttle_`name`
    argument and controlled by a LeakyBucket that guarantees it won't
    go any faster.

    Every `sync_interval` seconds the current bandwidth reported by
    the BandwidthCalculator instance is written into a row in a table
    shared with other instances of IOThrottler. The cumulated
    bandwidth of all other instances is retrieved from the same table.
    If it exceeds `max_speed`, the LeakyBucket instance is reset to
    only allow max_speed/(number of instances) so that the total
    bandwidth is shared equally between instances.
    """

    def __init__(self, name, **kwargs):
        super().__init__(kwargs["base_dsn"], "throttler")
        self.name = name
        self.init_db()
        self.last_sync = 0
        self.max_speed = kwargs["throttle_" + name]
        self.bucket = LeakyBucket(self.max_speed)
        self.bandwidth = BandwidthCalculator()

    def init_db(self):
        self.create_tables()
        self.db = self.connect_database()
        self.cursor = self.db.cursor()
        self.cursor.execute(
            f"INSERT INTO t_{self.name} (updated, bytes) VALUES (%s, %s) RETURNING id",
            (datetime.datetime.now(), 0),
        )
        self.rowid = self.cursor.fetchone()[0]
        self.cursor.execute(
            f"SELECT * FROM t_{self.name} WHERE id = %s FOR UPDATE", (self.rowid,)
        )
        self.cursor.execute(
            f"DELETE FROM t_{self.name} WHERE id IN ("
            f"SELECT id FROM t_{self.name} WHERE updated < NOW() - INTERVAL '30 days' "
            " FOR UPDATE SKIP LOCKED)"
        )
        self.db.commit()
        self.sync_interval = 60

    @property
    def lock(self):
        return 9485433  # an arbitrary unique number

    @property
    def database_tables(self):
        return [
            f"""
        CREATE TABLE IF NOT EXISTS t_{self.name}(
            id SERIAL PRIMARY KEY,
            updated TIMESTAMP NOT NULL,
            bytes INTEGER NOT NULL
        )
        """,
        ]

    def download_info(self):
        self.cursor.execute(
            f"SELECT COUNT(*), SUM(bytes) FROM t_{self.name} "
            "WHERE bytes > 0 AND updated > NOW() - INTERVAL '5 minutes'"
        )
        return self.cursor.fetchone()

    def upload_info(self):
        bytes = int(self.bandwidth.get())
        logger.debug("%d: upload %s/s", self.rowid, bytes)
        self.cursor.execute(
            f"UPDATE t_{self.name} SET updated = %s, bytes = %s WHERE id = %s",
            (datetime.datetime.now(), bytes, self.rowid),
        )
        self.db.commit()

    def add(self, count):
        self.bucket.add(count)
        self.bandwidth.add(count)
        self.maybe_sync()

    def sync(self):
        self.upload_info()
        (others_count, total_usage) = self.download_info()
        logger.debug(
            "%d: sync others_count=%s total_usage=%s",
            self.rowid,
            others_count,
            total_usage,
        )
        if others_count > 0 and total_usage > self.max_speed:
            self.bucket.reset(self.max_speed / others_count)

    def maybe_sync(self):
        now = time.monotonic()
        if now - self.last_sync > self.sync_interval:
            self.sync()
            self.last_sync = now


class Throttler:
    """Throttle reads and writes to not exceed limits imposed by the
    `thottle_read` and `throttle_write` arguments, as measured by the
    cumulated bandwidth reported by each Throttler instance.
    """

    def __init__(self, **kwargs):
        DatabaseAdmin(kwargs["base_dsn"], "throttler").create_database()
        self.read = IOThrottler("read", **kwargs)
        self.write = IOThrottler("write", **kwargs)

    def throttle_get(self, fun, key):
        content = fun(key)
        self.read.add(len(content))
        return content

    def throttle_add(self, fun, obj_id, content):
        self.write.add(len(obj_id) + len(content))
        return fun(obj_id, content)
