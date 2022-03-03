# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio


def call_async(f, *args):
    """Calls an async coroutine from a synchronous function."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(f(*args))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
