# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio
import concurrent.futures
import logging
import os
import random
import time
from typing import Any, Dict, Union

import psycopg2

from swh.objstorage.backends.winery.objstorage import (
    WineryObjStorage,
    WineryReader,
    WineryWriter,
)
from swh.objstorage.backends.winery.stats import Stats
from swh.objstorage.factory import get_objstorage
from swh.objstorage.interface import ObjStorageInterface
from swh.objstorage.objstorage import compute_hash

logger = logging.getLogger(__name__)


def work(kind, storage: Union[ObjStorageInterface, Dict[str, Any]]):
    if isinstance(storage, dict):
        if kind == "ro":
            storage = {**storage, "readonly": True}
        storage = get_objstorage("winery", **storage)
    return Worker(storage).run(kind)


class Worker:
    def __init__(self, storage: ObjStorageInterface):
        assert isinstance(
            storage, WineryObjStorage
        ), f"winery_benchmark passed unexpected {storage.__class__.__name__}"
        self.stats = Stats(storage.winery.args.get("output_dir"))
        self.storage = storage

    def run(self, kind):
        getattr(self, kind)()
        return kind

    def ro(self):
        if not isinstance(self.storage.winery, WineryReader):
            raise ValueError(
                f"Running ro benchmark on {self.storage.winery.__class__.__name__}"
                ", expected read-only"
            )

        try:
            self._ro()
        except psycopg2.OperationalError:
            # It may happen when the database is dropped, just
            # conclude the read loop gracefully and move on
            pass

    def _ro(self):
        with self.storage.winery.base.db.cursor() as c:
            while True:
                c.execute(
                    "SELECT signature FROM signature2shard WHERE inflight = FALSE "
                    "ORDER BY random() LIMIT %s",
                    (self.storage.winery.args["ro_worker_max_request"],),
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

    def rw(self):
        if not isinstance(self.storage.winery, WineryWriter):
            raise ValueError(
                f"Running rw benchmark on {self.storage.winery.__class__.__name__}"
                ", expected read-write"
            )
        self.payloads_define()
        random_content = open("/dev/urandom", "rb")
        logger.info("Worker(rw, %s): start", os.getpid())
        start = time.time()
        count = 0
        while len(self.storage.winery.packers) == 0:
            content = random_content.read(random.choice(self.payloads))
            obj_id = compute_hash(content, "sha256")
            self.storage.add(content=content, obj_id=obj_id)
            if self.stats.stats_active:
                self.stats.stats_write(obj_id, content)
            count += 1
        logger.info("Worker(rw, %s): packing %s objects", os.getpid(), count)
        packer = self.storage.winery.packers[0]
        packer.join()
        assert packer.exitcode == 0
        elapsed = time.time() - start
        logger.info("Worker(rw, %s): finished (%.2fs)", os.getpid(), elapsed)


class Bench(object):
    def __init__(self, args):
        self.args = args

    def timer_start(self):
        self.start = time.time()

    def timeout(self):
        return time.time() - self.start > self.args["duration"]

    async def run(self):
        self.timer_start()

        loop = asyncio.get_running_loop()

        workers_count = self.args["rw_workers"] + self.args["ro_workers"]
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=workers_count
        ) as executor:
            logger.info("Bench.run: running")

            self.count = 0
            workers = set()

            def create_worker(kind):
                self.count += 1
                logger.info("Bench.run: launched %s worker number %s", kind, self.count)
                return loop.run_in_executor(executor, work, kind, self.args)

            for kind in ["rw"] * self.args["rw_workers"] + ["ro"] * self.args[
                "ro_workers"
            ]:
                workers.add(create_worker(kind))

            while len(workers) > 0:
                logger.info("Bench.run: waiting for %s workers", len(workers))
                current = workers
                done, pending = await asyncio.wait(
                    current, return_when=asyncio.FIRST_COMPLETED
                )
                workers = pending
                for task in done:
                    kind = task.result()
                    logger.info("Bench.run: worker %s complete", kind)
                    if not self.timeout():
                        workers.add(create_worker(kind))

            logger.info("Bench.run: finished")

        return self.count
