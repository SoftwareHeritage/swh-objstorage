# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import time
from typing import Callable

logger = logging.getLogger(__name__)


def sleep_exponential(
    min_duration: float, factor: float, max_duration: float, message: str
) -> Callable[[], Callable[[], None]]:
    """Return a function that returns a callback that sleeps `min_duration`,
    then increases that by `factor` at every call, up to `max_duration`."""
    if min(min_duration, max_duration) <= 0:
        raise ValueError("Cannot sleep for a negative amount of time")

    def sleep_factory() -> Callable[[], None]:
        duration = min(min_duration, max_duration)

        def sleep():
            nonlocal duration
            logger.debug("%s. Waiting for %s", message, duration)
            time.sleep(duration)

            duration *= factor
            if duration >= max_duration:
                duration = max_duration

        return sleep

    return sleep_factory
