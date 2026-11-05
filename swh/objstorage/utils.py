# Copyright (C) 2015-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio
import concurrent.futures
import contextlib
import threading
from typing import (
    Any,
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


class AbstractAsyncLoop:
    def __init__(self):
        raise NotImplementedError("AbstractAsyncLoop.__init__")

    def is_alive(self) -> bool:
        raise NotImplementedError

    def terminate(self):
        """Returns whether the async loop is not closed"""
        raise NotImplementedError

    def run_until_complete(self, aw: Awaitable[T], *, timeout=None) -> T:
        """Runs the given awaitable in the event loop, blocks until completes,
        and returns its results, like :meth:`asyncio.BaseEventLoop.run_until_complete`.
        """
        raise NotImplementedError

    def enter_async_context(
        self, cm: AsyncContextManager[T]
    ) -> Coroutine[None, None, T]:
        """Creates a task to enter asynchronously an asynchronous iterator, like
        :meth:`contextlib.AsyncExitStack.enter_async_context`."""
        raise NotImplementedError

    def iter_asynciterator(self, it: AsyncIterator[T]) -> Iterator[T]:
        raise NotImplementedError


class AsyncLoop(AbstractAsyncLoop):
    def __init__(self, *args, **kwargs):
        self._async_loop = asyncio.new_event_loop()
        self._async_exit_stack = contextlib.AsyncExitStack()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_details):
        self.terminate()

    def is_alive(self) -> bool:
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

        return self._async_loop.run_until_complete(aw)

    def enter_async_context(
        self, cm: AsyncContextManager[T]
    ) -> Coroutine[None, None, T]:
        return self._async_exit_stack.enter_async_context(cm)

    def iter_asynciterator(self, it: AsyncIterator[T]) -> Iterator[T]:
        """Turns an async iterator into a synchronous iterator."""
        while True:
            try:
                res = self._async_loop.run_until_complete(it.__anext__())
            except StopAsyncIteration:
                return
            yield res


class AsyncLoopThread(threading.Thread, AbstractAsyncLoop):
    def __init__(self, *args, **kwargs):
        super().__init__(daemon=True, *args, **kwargs)
        self._initialized = threading.Event()
        self._terminate = asyncio.Event()
        self._async_loop = None
        self._async_exit_stack = contextlib.AsyncExitStack()

    def start(self):
        super().start()
        self._initialized.wait()

    def run(self):
        self._async_loop = asyncio.new_event_loop()

        # tell the main thread we are ready to receive requests
        self._initialized.set()

        try:
            self._async_loop.run_until_complete(self._terminate.wait())
        finally:
            # finish running jobs
            self._async_loop.run_until_complete(self._async_exit_stack.aclose())
            self._async_loop.run_until_complete(self._async_loop.shutdown_asyncgens())
            self._async_loop.close()

    def run_until_complete(self, aw: Awaitable[T], *, timeout=None) -> T:
        """Runs the given awaitable in the thread, blocks until completes,
        and returns its results"""
        coro: Coroutine[Any, Any, T]

        if asyncio.coroutines.iscoroutine(aw):
            coro = aw
        else:

            async def f():
                return await aw

            coro = f()

        fut: concurrent.futures.Future[T] = asyncio.run_coroutine_threadsafe(
            coro, self._async_loop
        )
        return fut.result(timeout=timeout)

    def enter_async_context(
        self, cm: AsyncContextManager[T]
    ) -> Coroutine[None, None, T]:
        return self._async_exit_stack.enter_async_context(cm)

    def terminate(self, timeout=None):
        """Asks the thread to finish, and blocks until it does."""
        self._async_loop.call_soon_threadsafe(self._terminate.set)
        self.join(timeout=timeout)

    def __del__(self):
        self._terminate.set()
        if not self._async_loop.is_closed():
            self._async_loop.call_soon_threadsafe(self._async_loop.stop)

    def iter_asynciterator(self, it: AsyncIterator[T]) -> Iterator[T]:
        async def anext():
            return await it.__anext__()

        while True:
            try:
                fut = asyncio.run_coroutine_threadsafe(anext(), self._async_loop)
                res = fut.result()
            except StopAsyncIteration:
                return
            yield res
