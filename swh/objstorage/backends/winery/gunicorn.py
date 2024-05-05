# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import sys

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
