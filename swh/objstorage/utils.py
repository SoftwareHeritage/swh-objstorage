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
    Callable,
    Coroutine,
    Dict,
    Iterator,
    List,
    Self,
    Tuple,
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


async def add_batch_async(
    add_async: Callable[[bytes, HashDict, bool], Awaitable[Tuple[int, int]]],
    contents: List[Tuple[HashDict, bytes]],
    check_presence: bool = True,
) -> Dict:
    """Calls ``add_async`` with each content in the list, and builds a ``summary`` dict
    from the pairs of ``(num_new_objects, num_added_bytes)`` it returns
    (ie. ``(0, 0)`` for known objects, ``(1, _)`` for new ones)"""
    try:
        results = await asyncio.gather(
            *[
                add_async(content, obj_id, check_presence)
                for (obj_id, content) in contents
            ]
        )

        summary = {"object:add": 0, "object:add:bytes": 0}
        for added, length in results:
            summary["object:add"] += added
            summary["object:add:bytes"] += length
    except Exception:
        import traceback

        traceback.print_exc()
        raise
    return summary
