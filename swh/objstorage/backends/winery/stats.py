# Copyright (C) 2022-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import logging
import os
import time

logger = logging.getLogger(__name__)


class Stats:
    def __init__(self, d):
        if d is None:
            self._stats_active = False
            return

        self._stats_active = True
        if not os.path.exists(d):
            try:
                os.makedirs(d)
            except FileExistsError:
                # This exception can happen, even when os.makedirs(d,
                # exist_ok=True) is used, when two concurrent processes try to
                # create the directory at the same time: one of the two
                # processes will receive the exception.
                pass
        self._stats = collections.Counter()
        self._stats_filename = f"{d}/{os.getpid()}.csv"
        self._stats_fd = open(self.stats_filename, "a")
        if self._stats_fd.tell() == 0:
            # We created the file, write the CSV header
            self._stats_fd.write(
                # time in seconds since epoch
                "time,"
                # total number of objects written at this point in time
                "object_write_count,"
                # total number of bytes written at this point in time
                "bytes_write,"
                # total number of objects read at this point in time
                "object_read_count,"
                # total number of bytes read at this point in time
                "bytes_read"
                "\n"
            )
        else:
            # The PID was recycled, print an empty stats line at the end of the file to mark it
            self._stats_print()

        self._stats_last_write = time.monotonic()
        self._stats_flush_interval = 5

    @property
    def stats_active(self):
        return self._stats_active

    @property
    def stats_filename(self):
        return self._stats_filename

    def __del__(self):
        if self.stats_active and not self._stats_fd.closed:
            self._stats_print()
            self._stats_fd.close()

    def _stats_print(self):
        ll = ",".join(
            str(self._stats[x])
            for x in [
                "object_write_count",
                "bytes_write",
                "object_read_count",
                "bytes_read",
            ]
        )
        self._stats_fd.write(f"{int(time.time())},{ll}\n")

    def _stats_maybe_print(self):
        now = time.monotonic()
        if now - self._stats_last_write > self._stats_flush_interval:
            self._stats_print()
            self._stats_last_write = now

    def stats_read(self, key, content):
        self._stats["object_read_count"] += 1
        self._stats["bytes_read"] += len(key) + len(content)
        self._stats_maybe_print()

    def stats_write(self, key, content):
        self._stats["object_write_count"] += 1
        self._stats["bytes_write"] += len(key) + len(content)
        self._stats_maybe_print()
