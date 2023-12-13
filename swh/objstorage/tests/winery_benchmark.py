# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio
import concurrent.futures
import logging
import os
import random
import sys
import time
from typing import Any, Dict, Optional, Set, Union

import psycopg2
from typing_extensions import Literal

from swh.objstorage.backends.winery.objstorage import (
    WineryObjStorage,
    WineryReader,
    WineryWriter,
    shard_packer,
)
from swh.objstorage.backends.winery.stats import Stats
from swh.objstorage.factory import get_objstorage
from swh.objstorage.interface import ObjStorageInterface
from swh.objstorage.objstorage import compute_hash

logger = logging.getLogger(__name__)

WorkerKind = Literal["ro", "rw", "pack"]


def work(
    kind: WorkerKind,
    storage: Union[ObjStorageInterface, Dict[str, Any]],
    worker_args: Optional[Dict[WorkerKind, Any]] = None,
) -> WorkerKind:
    if isinstance(storage, dict):
        if kind == "ro":
            storage = {**storage, "readonly": True}
        storage = get_objstorage("winery", **storage)

    if not worker_args:
        worker_args = {}

    kind_args = worker_args.get(kind, {})

    if kind == "ro":
        return ROWorker(storage, **kind_args).run()
    elif kind == "rw":
        return RWWorker(storage, **kind_args).run()
    elif kind == "pack":
        return PackWorker(**kind_args).run()
    else:
        raise ValueError("Unknown worker kind: %s" % kind)


class Worker:
    def __init__(self, storage: ObjStorageInterface):
        assert isinstance(
            storage, WineryObjStorage
        ), f"winery_benchmark passed unexpected {storage.__class__.__name__}"
        self.stats = Stats(storage.winery.args.get("output_dir"))
        self.storage = storage

    def run(self) -> WorkerKind:
        raise NotImplementedError


class PackWorker:
    def __init__(
        self,
        base_dsn: str,
        shard_dsn: str,
        shard_max_size: int,
        throttle_read: int,
        throttle_write: int,
        rbd_pool_name: str = "shards",
        output_dir: Optional[str] = None,
    ):
        self.base_dsn = base_dsn
        self.shard_dsn = shard_dsn
        self.shard_max_size = shard_max_size
        self.output_dir = output_dir
        self.throttle_read = throttle_read
        self.throttle_write = throttle_write
        self.rbd_pool_name = rbd_pool_name
        self.waited = 0

    def stop_packing(self, shards_count: int) -> bool:
        return shards_count >= 1 or self.waited > 5

    def wait_for_shard(self):
        time.sleep(1)
        self.waited += 1

    def run(self) -> Literal["pack"]:
        shard_packer(
            base_dsn=self.base_dsn,
            shard_dsn=self.shard_dsn,
            shard_max_size=self.shard_max_size,
            throttle_read=self.throttle_read,
            throttle_write=self.throttle_write,
            rbd_pool_name=self.rbd_pool_name,
            output_dir=self.output_dir,
            stop_packing=self.stop_packing,
            wait_for_shard_factory=lambda: self.wait_for_shard,
        )
        return "pack"


class ROWorker(Worker):
    def __init__(self, storage: ObjStorageInterface, max_request: int = 1000) -> None:
        super().__init__(storage)

        if not isinstance(self.storage.winery, WineryReader):
            raise ValueError(
                f"Running ro benchmark on {self.storage.winery.__class__.__name__}"
                ", expected read-only"
            )

        self.winery: WineryReader = self.storage.winery
        self.max_request = max_request

    def run(self) -> Literal["ro"]:
        try:
            self._ro()
        except psycopg2.OperationalError:
            # It may happen when the database is dropped, just
            # conclude the read loop gracefully and move on
            pass

        return "ro"

    def _ro(self):
        with self.storage.winery.base.db.cursor() as c:
            while True:
                c.execute(
                    "SELECT signature FROM signature2shard WHERE inflight = FALSE "
                    "ORDER BY random() LIMIT %s",
                    (self.max_request,),
                )
                if c.rowcount > 0:
                    break
                logger.info("Worker(ro, %s): empty, waiting", os.getpid())
                time.sleep(1)
            logger.info(
                "Worker(ro, %s): requesting %s objects", os.getpid(), c.rowcount
            )
            start = time.time()
            for row in c:
                obj_id = row[0].tobytes()
                content = self.storage.get(obj_id)
                assert content is not None
                if self.stats.stats_active:
                    self.stats.stats_read(obj_id, content)
            elapsed = time.time() - start
            logger.info("Worker(ro, %s): finished (%.2fs)", os.getpid(), elapsed)


class RWWorker(Worker):
    """A read-write benchmark worker

    Args:
      storage: the read-write storage used
      object_limit: the number of objects written before stopping
      single_shard: stop when the worker switches to a new shard
      block_until_packed: whether to wait for shards to be packed before exiting
    """

    def __init__(
        self,
        storage: ObjStorageInterface,
        object_limit: Optional[int] = None,
        single_shard: bool = True,
        block_until_packed: bool = True,
    ) -> None:
        super().__init__(storage)

        if not isinstance(self.storage.winery, WineryWriter):
            raise ValueError(
                f"Running rw benchmark on {self.storage.winery.__class__.__name__}"
                ", expected read-write"
            )

        self.winery: WineryWriter = self.storage.winery
        self.object_limit = object_limit
        self.single_shard = single_shard
        self.block_until_packed = block_until_packed
        self.count = 0

    def payloads_define(self):
        self.payloads = [
            3 * 1024 + 1,
            3 * 1024 + 1,
            3 * 1024 + 1,
            3 * 1024 + 1,
            3 * 1024 + 1,
            10 * 1024 + 1,
            13 * 1024 + 1,
            16 * 1024 + 1,
            70 * 1024 + 1,
            80 * 1024 + 1,
        ]

    def run(self) -> Literal["rw"]:
        self.payloads_define()
        random_content = open("/dev/urandom", "rb")
        logger.info("Worker(rw, %s): start", os.getpid())
        start = time.time()
        while self.keep_going():
            content = random_content.read(random.choice(self.payloads))
            obj_id = compute_hash(content, "sha256")
            self.storage.add(content=content, obj_id=obj_id)
            if self.stats.stats_active:
                self.stats.stats_write(obj_id, content)
            self.count += 1
        self.finalize()
        elapsed = time.time() - start
        logger.info("Worker(rw, %s): finished (%.2fs)", os.getpid(), elapsed)

        return "rw"

    def keep_going(self) -> bool:
        if self.object_limit is not None and self.count >= self.object_limit:
            return False
        if self.single_shard and self.winery.shards_filled:
            return False

        return True

    def finalize(self):
        if not self.block_until_packed:
            return
        logger.info(
            "Worker(rw, %s): waiting for %s objects to be packed",
            os.getpid(),
            self.count,
        )
        for packer in self.winery.packers:
            packer.join()
            assert packer.exitcode == 0


class Bench(object):
    def __init__(
        self,
        storage_config: Union[ObjStorageInterface, Dict[str, Any]],
        duration: int,
        workers_per_kind: Dict[WorkerKind, int],
        worker_args: Optional[Dict[WorkerKind, Any]] = None,
    ) -> None:
        self.storage_config = storage_config
        self.duration = duration
        self.workers_per_kind = workers_per_kind
        self.worker_args = worker_args or {}

    def timer_start(self):
        self.start = time.time()

    def timeout(self) -> bool:
        return time.time() - self.start > self.duration

    async def run(self) -> int:
        self.timer_start()

        loop = asyncio.get_running_loop()

        workers_count = sum(self.workers_per_kind.values())

        with concurrent.futures.ProcessPoolExecutor(
            max_workers=workers_count
        ) as executor:
            logger.info("Bench.run: running")

            self.count = 0
            workers: "Set[asyncio.Future[WorkerKind]]" = set()

            def create_worker(kind: WorkerKind) -> "asyncio.Future[WorkerKind]":
                self.count += 1
                logger.info("Bench.run: launched %s worker number %s", kind, self.count)
                return loop.run_in_executor(
                    executor, work, kind, self.storage_config, self.worker_args
                )

            for kind, count in self.workers_per_kind.items():
                for _ in range(count):
                    workers.add(create_worker(kind))

            while len(workers) > 0:
                logger.info("Bench.run: waiting for %s workers", len(workers))
                current = workers
                done, pending = await asyncio.wait(
                    current, return_when=asyncio.FIRST_COMPLETED
                )
                workers = pending
                exceptions = list(filter(None, [task.exception() for task in done]))
                if exceptions:
                    for task in pending:
                        task.cancel()
                    if sys.version_info >= (3, 11):
                        raise BaseExceptionGroup(  # noqa: F821
                            "Some workers raised an exception", exceptions
                        )
                    else:
                        for exc in exceptions:
                            logger.error("Worker raised an exception", exc_info=exc)
                        raise exceptions[0]
                for task in done:
                    kind = task.result()
                    logger.info("Bench.run: worker %s complete", kind)
                    if not self.timeout():
                        workers.add(create_worker(kind))

            logger.info("Bench.run: finished")

        return self.count
