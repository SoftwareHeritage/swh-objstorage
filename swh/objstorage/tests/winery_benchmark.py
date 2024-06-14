# Copyright (C) 2021-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio
from collections import Counter
import concurrent.futures
import datetime
import logging
from multiprocessing import current_process
import os
import random
import sys
import time
from typing import Any, Dict, Optional, Set, Union

import psycopg
import psycopg_pool
from typing_extensions import Literal

from swh.objstorage.backends.winery.objstorage import (
    WineryObjStorage,
    WineryReader,
    WineryWriter,
    rw_shard_cleaner,
    shard_packer,
)
from swh.objstorage.backends.winery.roshard import Pool
from swh.objstorage.backends.winery.sharedbase import ShardState
from swh.objstorage.backends.winery.stats import Stats
from swh.objstorage.factory import get_objstorage
from swh.objstorage.interface import ObjStorageInterface
from swh.objstorage.objstorage import objid_for_content

logger = logging.getLogger(__name__)

WorkerKind = Literal["ro", "rw", "pack", "rbd", "rw_shard_cleaner", "stats"]


def work(
    kind: WorkerKind,
    storage: Union[ObjStorageInterface, Dict[str, Any]],
    time_remaining: datetime.timedelta,
    worker_args: Optional[Dict[WorkerKind, Any]] = None,
    worker_id: int = 0,
) -> WorkerKind:
    if not worker_args:
        worker_args = {}

    kind_args = worker_args.get(kind, {})

    process_name = f"Worker-{kind}-{worker_id}"
    process = current_process()
    if process and process.name != "MainProcess":
        process.name = process_name

    logger.info("Started process %s", process_name)

    application_name = f"Winery Benchmark {process_name}"

    if kind == "ro":
        try:
            if isinstance(storage, dict):
                storage = get_objstorage(
                    cls="winery",
                    application_name=application_name,
                    **{
                        **storage,
                        "readonly": True,
                    },
                )
            return ROWorker(storage, **kind_args).run(time_remaining=time_remaining)
        finally:
            if isinstance(storage, WineryObjStorage):
                storage.on_shutdown()
    elif kind == "rw":
        try:
            if isinstance(storage, dict):
                storage = get_objstorage(
                    cls="winery", application_name=application_name, **storage
                )
            return RWWorker(storage, **kind_args).run(time_remaining=time_remaining)
        finally:
            if isinstance(storage, WineryObjStorage):
                storage.on_shutdown()
    elif kind == "pack":
        return PackWorker(application_name=application_name, **kind_args).run()
    elif kind == "rbd":
        return RBDWorker(application_name=application_name, **kind_args).run()
    elif kind == "rw_shard_cleaner":
        return RWShardCleanerWorker(
            application_name=application_name, **kind_args
        ).run()
    elif kind == "stats":
        return StatsPrinter(application_name=application_name, **kind_args).run(
            time_remaining=time_remaining
        )
    else:
        raise ValueError("Unknown worker kind: %s" % kind)


class Worker:
    def __init__(self, storage: ObjStorageInterface):
        assert isinstance(
            storage, WineryObjStorage
        ), f"winery_benchmark passed unexpected {storage.__class__.__name__}"
        self.stats: Stats = Stats(storage.winery.args.get("output_dir"))
        self.storage: WineryObjStorage = storage

    def run(self, time_remaining: datetime.timedelta) -> WorkerKind:
        raise NotImplementedError


class PackWorker:
    def __init__(
        self,
        base_dsn: str,
        shard_max_size: int,
        throttle_read: int,
        throttle_write: int,
        application_name: Optional[str] = None,
        rbd_create_images: bool = True,
        rbd_pool_name: str = "shards",
        output_dir: Optional[str] = None,
    ):
        self.base_dsn = base_dsn
        self.shard_max_size = shard_max_size
        self.output_dir = output_dir
        self.throttle_read = throttle_read
        self.throttle_write = throttle_write
        self.rbd_create_images = rbd_create_images
        self.rbd_pool_name = rbd_pool_name
        self.application_name = application_name
        self.waited = 0

    def stop_packing(self, shards_count: int) -> bool:
        return shards_count >= 1 or self.waited > 60

    def wait_for_shard(self, attempt: int) -> None:
        if self.waited > 60:
            raise ValueError("Shard waited for too long")
        time.sleep(0.1)
        self.waited += 1

    def run(self) -> Literal["pack"]:
        shard_packer(
            base_dsn=self.base_dsn,
            shard_max_size=self.shard_max_size,
            throttle_read=self.throttle_read,
            throttle_write=self.throttle_write,
            rbd_pool_name=self.rbd_pool_name,
            rbd_create_images=self.rbd_create_images,
            rbd_wait_for_image=self.wait_for_shard,
            output_dir=self.output_dir,
            stop_packing=self.stop_packing,
            wait_for_shard=self.wait_for_shard,
            application_name=self.application_name,
        )
        return "pack"


class RBDWorker:
    def __init__(
        self,
        base_dsn: str,
        rbd_pool_name: str,
        rbd_map_options: str,
        shard_max_size: int,
        application_name: Optional[str] = None,
        duration: int = 10,
    ):
        self.base_dsn = base_dsn
        self.pool = Pool(
            shard_max_size=shard_max_size,
            rbd_pool_name=rbd_pool_name,
            rbd_map_options=rbd_map_options,
        )
        self.duration = duration
        self.started = time.monotonic()
        self.application_name = application_name
        self.waited = 0

    def wait_for_shard(self, attempt: int) -> None:
        time.sleep(1)
        self.waited += 1

    def stop_running(self) -> bool:
        return time.monotonic() > self.started + self.duration or self.waited > 5

    def run(self) -> Literal["rbd"]:
        self.pool.manage_images(
            base_dsn=self.base_dsn,
            manage_rw_images=True,
            wait_for_image=self.wait_for_shard,
            stop_running=self.stop_running,
            application_name=self.application_name,
        )
        return "rbd"


class RWShardCleanerWorker:
    def __init__(
        self,
        base_dsn: str,
        min_mapped_hosts: int = 1,
        application_name: Optional[str] = None,
        duration: int = 10,
    ):
        self.base_dsn = base_dsn
        self.min_mapped_hosts = min_mapped_hosts
        self.application_name = application_name
        self.duration = duration
        self.started = time.monotonic()
        self.waited = 0

    def stop_cleaning(self, num_cleaned: int) -> bool:
        return num_cleaned >= 1 or self.waited > 5

    def wait_for_shard(self, attempt: int) -> None:
        time.sleep(1)
        self.waited += 1

    def run(self) -> Literal["rw_shard_cleaner"]:
        rw_shard_cleaner(
            base_dsn=self.base_dsn,
            min_mapped_hosts=self.min_mapped_hosts,
            stop_cleaning=self.stop_cleaning,
            wait_for_shard=self.wait_for_shard,
            application_name=self.application_name,
        )
        return "rw_shard_cleaner"


class StatsPrinter:
    def __init__(
        self,
        base_dsn: str,
        shard_max_size: int,
        application_name: Optional[str] = None,
        interval: int = 5 * 60,
    ):
        self.base_dsn = base_dsn
        self.shard_max_size = shard_max_size
        self.interval = datetime.timedelta(seconds=interval)
        self.application_name = application_name or "Winery Benchmark Stats Printer"
        self.objects_per_shard: Dict[str, int] = {}

    def get_winery_reader(self) -> WineryReader:
        return WineryReader(
            base_dsn=self.base_dsn,
            shard_max_size=self.shard_max_size,
            application_name=self.application_name,
        )

    def run(self, time_remaining: datetime.timedelta) -> Literal["stats"]:
        try:
            return self._run(time_remaining)
        except Exception:
            logger.exception("StatsPrinter.run raised exception")
            return "stats"

    def _run(self, time_remaining: datetime.timedelta) -> Literal["stats"]:
        sleep = min(time_remaining, self.interval).total_seconds()
        if sleep > 1:
            time.sleep(sleep)

        winery = self.get_winery_reader()
        shards = list(winery.base.list_shards())
        shard_counts: Counter[ShardState] = Counter()

        printed_rw_header = False

        for shard_name, _ in shards:
            # Get a fresh version of the state again to try and avoid a race
            state = winery.base.get_shard_state(shard_name)
            shard_counts[state] += 1
            if state not in {ShardState.STANDBY, ShardState.WRITING}:
                if shard_name not in self.objects_per_shard:
                    self.objects_per_shard[shard_name] = winery.base.count_objects(
                        shard_name
                    )
            else:
                if not printed_rw_header:
                    logger.info("read-write shard stats:")
                    printed_rw_header = True

                objects = winery.base.count_objects(shard_name)
                try:
                    shard = winery.rwshard(shard_name)
                    size = shard.size
                except psycopg_pool.PoolTimeout:
                    logger.info(
                        "Shard %s got eaten by the rw shard cleaner, sorry", shard_name
                    )
                    size = 0
                logger.info(
                    " shard %s (state: %s): objects: %s, total_size: %.1f GiB (%2.1f%%)",
                    shard_name,
                    state.name,
                    objects,
                    size / (1024 * 1024 * 1024),
                    100 * size / self.shard_max_size,
                )

        logger.info(
            "Read-only shard stats: count: %s, objects: %s, total_size (est.): %.1f GiB",
            len(self.objects_per_shard),
            sum(self.objects_per_shard.values()),
            (len(self.objects_per_shard) * self.shard_max_size) / (1024 * 1024 * 1024),
        )
        logger.info(
            "Shard counts: %s",
            ", ".join(f"{state.name}: {shard_counts[state]}" for state in ShardState),
        )

        return "stats"


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

    def run(self, time_remaining: datetime.timedelta) -> Literal["ro"]:
        try:
            self._ro(time_remaining)
        except psycopg.OperationalError:
            # It may happen when the database is dropped, just
            # conclude the read loop gracefully and move on
            logger.exception("RO worker got exception...")
        finally:
            self.finalize()

        return "ro"

    def _ro(self, time_remaining: datetime.timedelta):
        cutoff = time.time() + time_remaining.total_seconds()
        remaining = self.max_request

        start = time.monotonic()
        tablesample = 0.1
        random_cutoff = 0.1
        while remaining:
            if time.time() > cutoff:
                break
            with self.storage.winery.base.pool.connection() as db:
                limit = min(remaining, 1000)
                c = db.execute(
                    """
                    WITH selected AS (
                      SELECT signature, random() r
                      FROM signature2shard TABLESAMPLE BERNOULLI (%s)
                      WHERE state = 'present' and random() < %s
                      LIMIT %s)
                    SELECT signature FROM selected ORDER BY r
                    """,
                    (
                        tablesample,
                        random_cutoff,
                        limit,
                    ),
                )

                if c.rowcount == 0:
                    logger.info(
                        "Worker(ro, %s): empty (tablesample=%s, random_cutoff=%s), sleeping",
                        os.getpid(),
                        tablesample,
                        random_cutoff,
                    )
                    tablesample = min(tablesample * 10, 100)
                    random_cutoff = min(random_cutoff * 3, 1)
                    time.sleep(1)
                    continue
                elif c.rowcount == limit:
                    tablesample = max(tablesample / 10, 0.1)
                    random_cutoff = max(random_cutoff / 3, 0.1)

                for (obj_id,) in c:
                    remaining -= 1
                    if time.time() > cutoff:
                        remaining = 0
                        break
                    content = self.storage.get(obj_id={"sha256": obj_id})
                    assert content is not None
                    if self.stats.stats_active:
                        self.stats.stats_read(obj_id, content)

        elapsed = time.monotonic() - start
        logger.info("Worker(ro, %s): finished (%.2fs)", os.getpid(), elapsed)

    def finalize(self):
        self.storage.on_shutdown()


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

    def run(self, time_remaining: datetime.timedelta) -> Literal["rw"]:
        end = time.monotonic() + time_remaining.total_seconds()
        self.payloads_define()
        random_content = open("/dev/urandom", "rb")
        logger.info("Worker(rw, %s): start", os.getpid())
        start = time.monotonic()
        while self.keep_going() and time.monotonic() < end:
            content = random_content.read(random.choice(self.payloads))
            obj_id = objid_for_content(content)
            self.storage.add(content=content, obj_id=obj_id)
            if self.stats.stats_active:
                self.stats.stats_write(obj_id, content)
            self.count += 1
        self.finalize()
        elapsed = time.monotonic() - start
        logger.info("Worker(rw, %s): finished (%.2fs)", os.getpid(), elapsed)

        return "rw"

    def keep_going(self) -> bool:
        if self.object_limit is not None and self.count >= self.object_limit:
            return False
        if self.single_shard and self.winery.shards_filled:
            return False

        return True

    def finalize(self):
        self.storage.on_shutdown()

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
        self.start = 0

    def timer_start(self):
        self.start = time.monotonic()

    def timeout(self) -> bool:
        return time.monotonic() - self.start > self.duration

    def time_remaining(self) -> datetime.timedelta:
        return datetime.timedelta(seconds=self.start + self.duration - time.monotonic())

    async def run(self) -> int:
        self.timer_start()

        loop = asyncio.get_running_loop()

        workers_count = sum(self.workers_per_kind.values())

        with concurrent.futures.ProcessPoolExecutor(
            max_workers=workers_count
        ) as executor:
            logger.info("Running winery benchmark")

            self.count = 0
            workers: "Set[asyncio.Future[WorkerKind]]" = set()

            def create_worker(kind: WorkerKind) -> "asyncio.Future[WorkerKind]":
                self.count += 1
                logger.info("launched %s worker number %s", kind, self.count)
                return loop.run_in_executor(
                    executor,
                    work,
                    kind,
                    self.storage_config,
                    self.time_remaining(),
                    self.worker_args,
                    self.count,
                )

            for kind, count in self.workers_per_kind.items():
                for _ in range(count):
                    workers.add(create_worker(kind))

            while len(workers) > 0:
                logger.info(
                    "Waiting for %s workers",
                    ", ".join(
                        f"{v} {k}" for k, v in self.workers_per_kind.items() if v
                    ),
                )
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
                    logger.info("worker %s complete", kind)
                    if not self.timeout():
                        workers.add(create_worker(kind))
                    else:
                        self.workers_per_kind[kind] -= 1

            logger.info("Bench.run: finished")

        return self.count
