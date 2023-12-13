# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import time

logger = logging.getLogger(__name__)


def sleep_exponential(
    min_duration: float, factor: float, max_duration: float, message: str
):
    """Return a function that sleeps `min_duration`,
    then increases that by `factor` at every call, up to `max_duration`."""
    duration = min(min_duration, max_duration)

    if duration <= 0:
        raise ValueError("Cannot sleep for a negative amount of time")

    def sleep():
        nonlocal duration
        logger.debug("%s. Waiting for %s", message, duration)
        time.sleep(duration)

        duration *= factor
        if duration >= max_duration:
            duration = max_duration

    return sleep
