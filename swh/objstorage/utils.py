# Copyright (C) 2021-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio
import contextlib
from typing import (
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Coroutine,
    Iterator,
    Self,
    TypeVar,
)

from swh.objstorage.interface import COMPOSITE_OBJID_KEYS, HashDict


def format_obj_id(obj_id: HashDict) -> str:
    return ";".join(
        (
            "%s:%s" % (algo, obj_id[algo].hex())
            for algo in sorted(COMPOSITE_OBJID_KEYS)
            if algo in obj_id
        )
    )


T = TypeVar("T")


class AsyncLoop:
    def __init__(self, *args, **kwargs):
        self._async_loop = asyncio.new_event_loop()
        self._async_exit_stack = contextlib.AsyncExitStack()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_details):
        self.terminate()

    def is_alive(self) -> bool:
        """Returns whether the async loop is not closed"""
        return not self._async_loop.is_closed()

    def terminate(self, timeout=None):
        """Asks the thread to finish, and blocks until it does."""
        self._async_loop.run_until_complete(self._async_exit_stack.aclose())
        self._async_loop.run_until_complete(self._async_loop.shutdown_asyncgens())
        self._async_loop.close()

    def __del__(self):
        if not self._async_loop.is_closed():
            self.terminate()

    def run_until_complete(self, aw: Awaitable[T], *, timeout=None) -> T:
        """Runs the given awaitable in the thread, blocks until completes,
        and returns its results, like :meth:`asyncio.BaseEventLoop.run_until_complete`.
        """

        return self._async_loop.run_until_complete(aw)

    def enter_async_context(
        self, cm: AsyncContextManager[T]
    ) -> Coroutine[None, None, T]:
        """Creates a task to enter asynchronously an asynchronous iterator, like
        :meth:`contextlib.AsyncExitStack.enter_async_context`."""
        return self._async_exit_stack.enter_async_context(cm)

    def iter_asynciterator(self, it: AsyncIterator[T]) -> Iterator[T]:
        """Turns an async iterator into a synchronous iterator."""
        while True:
            try:
                res = self._async_loop.run_until_complete(it.__anext__())
            except StopAsyncIteration:
                return
            yield res
