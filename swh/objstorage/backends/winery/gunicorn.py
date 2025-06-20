# Copyright (C) 2024-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter
import logging
import sys
from time import monotonic
from typing import Tuple

from swh.core.api.gunicorn_config import *  # noqa

logger = logging.getLogger(__name__)


def worker_exit(arbiter, worker):
    logger.info("Calling worker_exit")
    shutdown_storage_backend()


def worker_int(worker):
    logger.warning("Calling worker_int")
    shutdown_storage_backend()


def shutdown_storage_backend():
    """Run on_shutdown callback for storage when a worker is terminating"""
    if "swh.objstorage.api.server" not in sys.modules:
        return
    objstorage = sys.modules["swh.objstorage.api.server"].objstorage

    if on_shutdown := getattr(objstorage, "on_shutdown", None):
        on_shutdown()


class ThrottledAccessLog(logging.Filter):
    """Throttle gunicorn access log lines for `status_codes` to at most one
    every `interval` seconds"""

    def __init__(self, interval: int = 60, status_codes: Tuple[int, ...] = (200,)):
        super().__init__(name="gunicorn.access")
        self.status_codes = set(str(code) for code in status_codes)
        self.endpoints: Counter[str] = Counter()
        self.interval = interval

        self.previous_flush = monotonic()
        self.deadline = self.previous_flush + self.interval

    def filter(self, record):
        # gunicorn.access records are using `s` for status code and `U` for the
        # requested path
        if not (ret := super().filter(record)):
            return ret

        if record.args["s"] not in self.status_codes:
            return True

        # If we quiesce different status codes, stick them in different buckets
        if len(self.status_codes) > 1:
            bucket = f"{record.args['U']}({record.args['s']})"
        else:
            bucket = record.args["U"]

        self.endpoints[bucket] += 1

        now = monotonic()
        if self.deadline > now:
            # Quiesce record
            return False

        # logging.Filter must mutate the log record instead of creating a
        # new one, up to and including Python 3.11
        record.msg = (
            "Served %(total)s requests in the last %(interval).1fs, "
            "including %(most_common)s"
        )
        record.args = {
            "total": self.endpoints.total(),
            "most_common": ", ".join(
                f"{v} {path}" for path, v in self.endpoints.most_common(3)
            ),
            "interval": now - self.previous_flush,
        }
        self.previous_flush = now
        self.deadline = now + self.interval
        self.endpoints = Counter()
        return True
