# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
from typing import Callable, TypeVar

from swh.core import statsd

DURATION_METRIC = "swh_objstorage_request_duration_seconds"

F = TypeVar("F", bound=Callable)


def timed(f: F) -> F:
    """A simple decorator used to add statsd probes on main ObjStorage methods
    (add, get and __contains__)
    """
    tags = {"endpoint": f.__name__}

    @functools.wraps(f)
    def newf(self, *args, **kw):
        with statsd.statsd.timed(DURATION_METRIC, tags={"name": self.name, **tags}):
            return f(self, *args, **kw)

    return newf  # type: ignore[return-value]
