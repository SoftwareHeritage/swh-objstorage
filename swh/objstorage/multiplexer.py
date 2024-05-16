# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import queue
import threading
from typing import Dict, Iterable, Iterator, Mapping, Optional, Tuple, Union

from swh.model.model import Sha1
from swh.objstorage.exc import ObjCorruptedError, ObjNotFoundError
from swh.objstorage.factory import get_objstorage
from swh.objstorage.interface import CompositeObjId, ObjId, ObjStorageInterface
from swh.objstorage.objstorage import ObjStorage
from swh.objstorage.utils import format_obj_id

logger = logging.getLogger(__name__)


class ObjStorageThread(threading.Thread):
    def __init__(self, storage):
        super().__init__(daemon=True)
        self.storage = storage
        self.commands = queue.Queue()

    def run(self):
        while True:
            try:
                mailbox, command, args, kwargs = self.commands.get(True, 0.05)
            except queue.Empty:
                continue

            try:
                ret = getattr(self.storage, command)(*args, **kwargs)
            except Exception as exc:
                self.queue_result(mailbox, "exception", exc)
            else:
                self.queue_result(mailbox, "result", ret)

    def queue_command(self, command, *args, mailbox=None, **kwargs):
        """Enqueue a new command to be processed by the thread.

        Args:
          command (str): one of the method names for the underlying storage.
          mailbox (queue.Queue): explicit mailbox if the calling thread wants
            to override it.
          args, kwargs: arguments for the command.

        Returns:
          queue.Queue: The mailbox you can read the response from
        """
        if not mailbox:
            mailbox = queue.Queue()
        self.commands.put((mailbox, command, args, kwargs))
        return mailbox

    def queue_result(self, mailbox, result_type, result):
        """Enqueue a new result in the mailbox

        This also provides a reference to the storage, which can be useful when
        an exceptional condition arises.

        Args:
          mailbox (queue.Queue): the mailbox to which we need to enqueue the
            result
          result_type (str): one of 'result', 'exception'
          result: the result to pass back to the calling thread
        """
        mailbox.put(
            {
                "type": result_type,
                "result": result,
            }
        )

    @staticmethod
    def get_result_from_mailbox(mailbox, *args, **kwargs):
        """Unpack the result from the mailbox.

        Args:
          mailbox (queue.Queue): A mailbox to unpack a result from
          args: positional arguments to :func:`mailbox.get`
          kwargs: keyword arguments to :func:`mailbox.get`

        Returns:
          the next result unpacked from the queue

        Raises:
          either the exception we got back from the underlying storage,
            or :exc:`queue.Empty` if :func:`mailbox.get` raises that.
        """

        result = mailbox.get(*args, **kwargs)
        if result["type"] == "exception":
            raise result["result"] from None
        else:
            return result["result"]

    @staticmethod
    def collect_results(mailbox, num_results):
        """Collect num_results from the mailbox"""
        collected = 0
        ret = []
        while collected < num_results:
            try:
                ret.append(
                    ObjStorageThread.get_result_from_mailbox(mailbox, True, 0.05)
                )
            except queue.Empty:
                continue
            collected += 1
        return ret

    def __getattr__(self, attr):
        def call(*args, **kwargs):
            mailbox = self.queue_command(attr, *args, **kwargs)
            return self.get_result_from_mailbox(mailbox)

        return call

    def __contains__(self, *args, **kwargs):
        mailbox = self.queue_command("__contains__", *args, **kwargs)
        return self.get_result_from_mailbox(mailbox)


class MultiplexerObjStorage(ObjStorage):
    """Implementation of ObjStorage that distributes between multiple
    storages.

    The multiplexer object storage allows an input to be demultiplexed
    among multiple storages that will or will not accept it by
    themselves.

    As the ids can be different, no pre-computed ids should be
    submitted.  Also, there are no guarantees that the returned ids
    can be used directly into the storages that the multiplexer
    manage.

    Use case examples follow.

    Example::

        storage_v1 = ReadOnlyProxyObjStorage(
                        PathSlicingObjStorage('/dir1', '0:2/2:4/4:6')
        )
        storage_v2 = PathSlicingObjStorage('/dir2', '0:1/0:5')
        storage = MultiplexerObjStorage([storage_v1, storage_v2])

    When using 'storage', all the new contents will only be added to the v2
    storage, while it will be retrievable from both.

    """

    name: str = "multiplexer"

    def __init__(
        self,
        *,
        objstorages: Iterable[Union[ObjStorageInterface, Dict]],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.storages = [
            get_objstorage(**sto) if isinstance(sto, dict) else sto
            for sto in objstorages
        ]
        self.storage_threads = [ObjStorageThread(storage) for storage in self.storages]
        for thread in self.storage_threads:
            thread.start()
        self.write_storage_threads = [
            thread
            for thread, storage in zip(self.storage_threads, self.storages)
            if storage.check_config(check_write=True)
        ]

    def wrap_call(self, threads, call, *args, **kwargs):
        threads = list(threads)
        mailbox = queue.Queue()
        for thread in threads:
            thread.queue_command(call, *args, mailbox=mailbox, **kwargs)

        return ObjStorageThread.collect_results(mailbox, len(threads))

    def get_read_threads(self, obj_id=None):
        yield from self.storage_threads

    def get_write_threads(self, obj_id=None):
        yield from self.write_storage_threads

    def check_config(self, *, check_write):
        """Check whether the object storage is properly configured.

        If check_write is True, return True if at least one object storage
        returned True.

        Args:
            check_write (bool): if True, check if writes to the object storage
            can succeed.

        Returns:
            True if the configuration check worked, an exception if it didn't.

        """
        if not check_write:
            return all(
                self.wrap_call(
                    self.storage_threads, "check_config", check_write=check_write
                )
            )
        else:
            return any(
                self.wrap_call(
                    self.storage_threads, "check_config", check_write=check_write
                )
            )

    def __contains__(self, obj_id: ObjId) -> bool:
        """Indicate if the given object is present in the storage.

        Args:
            obj_id (bytes): object identifier.

        Returns:
            True if and only if the object is present in the current object
            storage.

        """
        for storage in self.get_read_threads(obj_id):
            if obj_id in storage:
                return True
        return False

    def __iter__(self) -> Iterator[CompositeObjId]:
        def obj_iterator():
            for storage in self.storages:
                yield from storage

        return obj_iterator()

    def add(self, content: bytes, obj_id: ObjId, check_presence: bool = True) -> None:
        """Add a new object to the object storage.

        If the adding step works in all the storages that accept this content,
        this is a success. Otherwise, the full adding step is an error even if
        it succeed in some of the storages.

        Args:
            content: content of the object to be added to the storage.
            obj_id: checksum of [bytes] using [ID_HASH_ALGO] algorithm. When
                given, obj_id will be trusted to match the bytes. If missing,
                obj_id will be computed on the fly.
            check_presence: indicate if the presence of the content should be
                verified before adding the file.

        Returns:
            an id of the object into the storage. As the write-storages are
            always readable as well, any id will be valid to retrieve a
            content.
        """
        self.wrap_call(
            self.get_write_threads(obj_id),
            "add",
            content,
            obj_id=obj_id,
            check_presence=check_presence,
        )

    def add_batch(
        self,
        contents: Union[Mapping[Sha1, bytes], Iterable[Tuple[ObjId, bytes]]],
        check_presence: bool = True,
    ) -> Dict:
        """Add a batch of new objects to the object storage."""
        write_threads = list(self.get_write_threads())
        results = self.wrap_call(
            write_threads,
            "add_batch",
            contents,
            check_presence=check_presence,
        )

        summed = {"object:add": 0, "object:add:bytes": 0}
        for result in results:
            summed["object:add"] += result["object:add"]
            summed["object:add:bytes"] += result["object:add:bytes"]

        return {
            "object:add": summed["object:add"] // len(results),
            "object:add:bytes": summed["object:add:bytes"] // len(results),
        }

    def restore(self, content: bytes, obj_id: ObjId) -> None:
        return self.wrap_call(
            self.get_write_threads(obj_id),
            "restore",
            content,
            obj_id=obj_id,
        ).pop()

    def get(self, obj_id: ObjId) -> bytes:
        corrupted_exc: Optional[ObjCorruptedError] = None
        for storage in self.get_read_threads(obj_id):
            try:
                return storage.get(obj_id)
            except ObjNotFoundError:
                continue
            except ObjCorruptedError as exc:
                logger.warning(
                    "Object %s was reported as corrupted by backend '%s': %s",
                    format_obj_id(obj_id),
                    storage.storage.name,
                    str(exc),
                )
                # Hoist exception, mypy doesn't like when we directly use
                # `except ObjCorruptedError as corrupted_exc`
                corrupted_exc = exc
                # Try reading from another storage
                continue

        if corrupted_exc:
            # The only objects we've found were corrupted, raise that exception
            raise corrupted_exc
        else:
            # No storage contains this content, raise the error
            raise ObjNotFoundError(obj_id)

    def check(self, obj_id: ObjId) -> None:
        nb_present = 0
        nb_corrupted = 0
        exception: Optional[Exception] = None
        for storage in self.get_read_threads(obj_id):
            try:
                storage.check(obj_id)
            except ObjNotFoundError as e:
                exception = e
                continue
            except ObjCorruptedError as e:
                exception = e
                nb_corrupted += 1
            else:
                nb_present += 1

        # Raise exception only if the content could not be found in all the storages
        # or is corrupted in all the storages
        if exception and (nb_present == 0 or nb_corrupted == len(self.storages)):
            raise exception

    def delete(self, obj_id: ObjId):
        super().delete(obj_id)  # Check delete permission
        return all(self.wrap_call(self.get_write_threads(obj_id), "delete", obj_id))
