# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import re
from typing import Dict, List, Tuple

import pytest

from swh.objstorage.backends.in_memory import InMemoryObjStorage
from swh.objstorage.exc import (
    NoBackendsLeftError,
    ObjCorruptedError,
    ReadOnlyObjStorageError,
)
from swh.objstorage.factory import get_objstorage
from swh.objstorage.multiplexer import (
    MP_BACKEND_DISABLED_METRICS,
    MP_BACKEND_ENABLED_METRICS,
    MP_COUNTER_METRICS,
    MultiplexerObjStorage,
)
from swh.objstorage.objstorage import DURATION_METRICS, objid_for_content

from .objstorage_testing import ObjStorageTestFixture


def statsd_payloads_having_tags(
    statsd, **tags: str
) -> List[Tuple[str, Dict[str, str]]]:
    ret = []
    for s in statsd.socket.payloads:
        m = re.fullmatch(
            r"^(?P<metric_name>[^:]+):(?P<value>[^|]+)[|](?P<unit>ms|c)[|](#(?P<tags>.*))?$",
            s.decode(),
        )
        if not m:
            continue
        payload_tags = {}
        for tag in m.group("tags").split(","):
            tag, value = tag.split(":", 1)
            payload_tags[tag] = value
        if tags.items() <= payload_tags.items():
            ret.append((m.group("metric_name"), payload_tags))
    return ret


def clear_statsd_payloads(statsd) -> None:
    statsd.socket.payloads.clear()


class TestMultiplexerObjStorage(ObjStorageTestFixture):

    @pytest.fixture
    def swh_objstorage(request, swh_objstorage_config):
        """Fixture that instantiates an object storage based on the configuration
        returned by the ``swh_objstorage_config`` fixture.

        Overloaded here to get rid of the primary_hash parametrization
        """
        return get_objstorage(**swh_objstorage_config)

    @pytest.fixture
    def swh_objstorage_config(self, tmpdir):
        root1 = os.path.join(tmpdir, "root1")
        root2 = os.path.join(tmpdir, "root2")
        os.mkdir(root1)
        os.mkdir(root2)
        return {
            "cls": "multiplexer",
            "objstorages": [
                {
                    "cls": "read-only",
                    "name": "ro_backend",
                    "storage": {
                        "cls": "pathslicing",
                        "name": "pathslicer_1",
                        "root": root1,
                        "slicing": "0:2/2:4",
                    },
                },
                {
                    "cls": "pathslicing",
                    "name": "rw_backend",
                    "root": root2,
                    "slicing": "0:1/0:5",
                    "primary_hash": "sha256",
                },
            ],
        }

    @pytest.mark.skip(reason="Unsupported by the multiplexer")
    def test_list_content_all(self):
        pass

    @pytest.mark.skip(reason="Unsupported by the multiplexer")
    def test_list_content_limit(self):
        pass

    @pytest.mark.skip(reason="Unsupported by the multiplexer")
    def test_list_content_limit_and_last(self):
        pass

    def test_contains(self):
        content_p, obj_id_p = self.hash_content(b"contains_present")
        content_m, obj_id_m = self.hash_content(b"contains_missing")
        self.storage.add(content_p, obj_id=obj_id_p)
        assert obj_id_p in self.storage
        assert obj_id_m not in self.storage

    @pytest.fixture
    def allow_delete(self):
        for storage in self.storage.storages:
            storage.allow_delete = True

    def test_delete_missing(self, allow_delete):
        super().test_delete_missing()

    def test_delete_present(self, allow_delete):
        super().test_delete_present()

    def test_access_readonly(self):
        # Add a content to the readonly storage
        content, obj_id = self.hash_content(b"content in read-only")
        with pytest.raises(ReadOnlyObjStorageError):
            self.storage.storages[0].add(content, obj_id=obj_id)
        # Try to retrieve it on the main storage
        assert obj_id not in self.storage

    def test_get_statsd_multiplexer(self, statsd):
        content1, obj_id1 = self.hash_content(b"add_get_batch_1")
        content2, obj_id2 = self.hash_content(b"add_get_batch_2")
        content3, obj_id3 = self.hash_content(b"add_get_batch_3")
        self.storage.add(content1, obj_id1)
        self.storage.add(content2, obj_id2)
        expected_payloads = [
            # first call to add()
            (DURATION_METRICS, {"endpoint": "add", "name": "multiplexer"}),
            # second call to add()
            (DURATION_METRICS, {"endpoint": "add", "name": "multiplexer"}),
        ]
        payloads = statsd_payloads_having_tags(statsd, name="multiplexer")
        assert payloads == expected_payloads

        # add a content in the RO backend
        self.storage.storages[0].storage.add(content3, obj_id3)
        clear_statsd_payloads(statsd)

        self.storage.get(obj_id1)
        self.storage.get(obj_id2)
        self.storage.get(obj_id3)

        # check metrics reported by the multiplexer itself
        expected_mp_payloads = [
            # first get()
            (
                MP_COUNTER_METRICS,
                {
                    "backend": "rw_backend",
                    "backend_number": "1",
                    "endpoint": "get",
                    "name": "multiplexer",
                },
            ),
            (DURATION_METRICS, {"endpoint": "get", "name": "multiplexer"}),
            # second get()
            (
                MP_COUNTER_METRICS,
                {
                    "backend": "rw_backend",
                    "backend_number": "1",
                    "endpoint": "get",
                    "name": "multiplexer",
                },
            ),
            (DURATION_METRICS, {"endpoint": "get", "name": "multiplexer"}),
            # third get()
            (
                MP_COUNTER_METRICS,
                {
                    "backend": "ro_backend",
                    "backend_number": "0",
                    "endpoint": "get",
                    "name": "multiplexer",
                },
            ),
            (DURATION_METRICS, {"endpoint": "get", "name": "multiplexer"}),
        ]
        mp_payloads = statsd_payloads_having_tags(statsd, name="multiplexer")
        assert mp_payloads == expected_mp_payloads

        # check metrics reported by the ro backend (aka pathslicer_1)
        expected_ro_payloads = [
            # first get()
            (DURATION_METRICS, {"endpoint": "__contains__", "name": "pathslicer_1"}),
            (
                f"{DURATION_METRICS}_error_count",
                {
                    "endpoint": "get",
                    "error_type": "ObjNotFoundError",
                    "name": "pathslicer_1",
                },
            ),
            # second get()
            (DURATION_METRICS, {"endpoint": "__contains__", "name": "pathslicer_1"}),
            (
                f"{DURATION_METRICS}_error_count",
                {
                    "endpoint": "get",
                    "error_type": "ObjNotFoundError",
                    "name": "pathslicer_1",
                },
            ),
            # third get()
            (DURATION_METRICS, {"endpoint": "__contains__", "name": "pathslicer_1"}),
            (DURATION_METRICS, {"endpoint": "get", "name": "pathslicer_1"}),
        ]
        ro_payloads = statsd_payloads_having_tags(statsd, name="pathslicer_1")
        assert ro_payloads == expected_ro_payloads

        # check metrics reported by the rw backend (aka rw_backend)
        expected_rw_payloads = [
            # first get()
            (DURATION_METRICS, {"endpoint": "__contains__", "name": "rw_backend"}),
            (DURATION_METRICS, {"endpoint": "get", "name": "rw_backend"}),
            # second get()
            (DURATION_METRICS, {"endpoint": "__contains__", "name": "rw_backend"}),
            (DURATION_METRICS, {"endpoint": "get", "name": "rw_backend"}),
        ]
        rw_payloads = statsd_payloads_having_tags(statsd, name="rw_backend")
        assert rw_payloads == expected_rw_payloads

        # clear the statsd messages
        statsd.socket.payloads.clear()
        # and check metrics for the __contains__ method
        assert obj_id1 in self.storage
        assert obj_id2 in self.storage
        assert obj_id3 in self.storage

        # for now, there is no counter (MP_COUNTER_METRICS) on any other method
        # than get, so we don't have any simple stat on who answered the
        # request...
        expected_mp_payloads = [
            (DURATION_METRICS, {"endpoint": "__contains__", "name": "multiplexer"}),
            (DURATION_METRICS, {"endpoint": "__contains__", "name": "multiplexer"}),
            (DURATION_METRICS, {"endpoint": "__contains__", "name": "multiplexer"}),
        ]
        mp_payloads = statsd_payloads_having_tags(statsd, name="multiplexer")
        assert mp_payloads == expected_mp_payloads

        expected_ro_payloads = [
            (DURATION_METRICS, {"endpoint": "__contains__", "name": "pathslicer_1"}),
            (DURATION_METRICS, {"endpoint": "__contains__", "name": "pathslicer_1"}),
            (DURATION_METRICS, {"endpoint": "__contains__", "name": "pathslicer_1"}),
        ]
        ro_payloads = statsd_payloads_having_tags(statsd, name="pathslicer_1")
        assert ro_payloads == expected_ro_payloads

        expected_rw_payloads = [
            (DURATION_METRICS, {"endpoint": "__contains__", "name": "rw_backend"}),
            (DURATION_METRICS, {"endpoint": "__contains__", "name": "rw_backend"}),
        ]
        rw_payloads = statsd_payloads_having_tags(statsd, name="rw_backend")
        assert rw_payloads == expected_rw_payloads


def test_multiplexer_corruption_fallback(mocker, caplog):
    content_p = b"contains_present"
    obj_id_p = objid_for_content(content_p)

    class CorruptedInMemoryObjStorage(InMemoryObjStorage):
        name = "corrupted_objstorage"

        def get(self, obj_id):
            raise ObjCorruptedError("Always corrupted", obj_id)

    corrupt_storage = CorruptedInMemoryObjStorage()
    corrupt_get = mocker.spy(corrupt_storage, "get")

    ok_storage = InMemoryObjStorage()
    ok_get = mocker.spy(ok_storage, "get")

    multiplexer = MultiplexerObjStorage(objstorages=[corrupt_storage, ok_storage])
    multiplexer.add(content_p, obj_id=obj_id_p)

    assert obj_id_p in corrupt_storage
    assert obj_id_p in ok_storage

    with (
        caplog.at_level(logging.WARNING, "swh.core.statsd"),
        caplog.at_level(logging.WARNING, "swh.objstorage.multiplexer"),
    ):
        assert multiplexer.get(obj_id_p) == content_p

    corrupt_get.assert_called_once_with(obj_id_p)
    ok_get.assert_called_once_with(obj_id_p)

    assert len(caplog.records) == 1
    assert (
        "was reported as corrupted by backend 'corrupted_objstorage'"
        in caplog.records[0].message
    )


def test_multiplexer_transient_error_fallback(mocker, caplog, statsd):
    content_p = b"contains_present"
    obj_id_p = objid_for_content(content_p)

    class TimeoutInMemoryObjStorage(InMemoryObjStorage):
        name = "timeout-in-memory"

        def get(self, obj_id):
            raise TimeoutError("Always timeout", obj_id)

    timeout_storage = TimeoutInMemoryObjStorage(name="always-timeout")
    timeout_get = mocker.spy(timeout_storage, "get")

    ok_storage = InMemoryObjStorage()
    ok_get = mocker.spy(ok_storage, "get")

    multiplexer = MultiplexerObjStorage(
        objstorages=[timeout_storage, ok_storage],
        read_exception_cooldown=2,
        name="my-multiplexer",
    )
    multiplexer.add(content_p, obj_id=obj_id_p)

    assert obj_id_p in timeout_storage
    assert obj_id_p in ok_storage

    with caplog.at_level(
        logging.WARNING, "swh.objstorage.multiplexer.multiplexer_objstorage"
    ):
        assert multiplexer.get(obj_id_p) == content_p

    timeout_get.assert_called_once_with(obj_id_p)
    ok_get.assert_called_once_with(obj_id_p)

    assert len(caplog.records) == 1
    assert "always-timeout" in caplog.records[0].message
    assert "transient" in caplog.records[0].message
    for algo, hash in obj_id_p.items():
        assert f"{algo}:{hash.hex()}" in caplog.records[0].message
    # We don’t want to see a full stack trace as it will very likely
    # be noise as it is a transient error.
    assert "raise TimeoutError" not in caplog.text
    # But we still want to know which exception it was
    assert "TimeoutError" in caplog.text

    assert 0 in multiplexer.reset_timers

    assert (
        MP_BACKEND_DISABLED_METRICS,
        {
            "backend": "always-timeout",
            "backend_number": "0",
            "endpoint": "get",
            "name": "my-multiplexer",
        },
    ) in statsd_payloads_having_tags(statsd, name="my-multiplexer")

    clear_statsd_payloads(statsd)

    timeout_get.reset_mock()
    assert multiplexer.get(obj_id_p) == content_p

    timeout_get.assert_not_called()

    assert MP_BACKEND_DISABLED_METRICS not in (
        metric
        for metric, tags in statsd_payloads_having_tags(statsd, name="my-multiplexer")
    )
    clear_statsd_payloads(statsd)

    multiplexer.enable_backend(name="always-timeout", i=0)

    assert (
        MP_BACKEND_ENABLED_METRICS,
        {
            "backend": "always-timeout",
            "backend_number": "0",
            "name": "my-multiplexer",
        },
    ) in statsd_payloads_having_tags(statsd, name="my-multiplexer")
    clear_statsd_payloads(statsd)

    assert multiplexer.get(obj_id_p) == content_p
    timeout_get.assert_called_once_with(obj_id_p)

    assert (
        MP_BACKEND_DISABLED_METRICS,
        {
            "backend": "always-timeout",
            "backend_number": "0",
            "endpoint": "get",
            "name": "my-multiplexer",
        },
    ) in statsd_payloads_having_tags(statsd, name="my-multiplexer")


def test_multiplexer_transient_error_nobackendsleft(mocker, caplog):
    content_p = b"contains_present"
    obj_id_p = objid_for_content(content_p)

    class TimeoutInMemoryObjStorage(InMemoryObjStorage):
        def get(self, obj_id):
            raise TimeoutError("Always timeout", obj_id)

    multiplexer = MultiplexerObjStorage(objstorages=[TimeoutInMemoryObjStorage()])
    multiplexer.add(content_p, obj_id=obj_id_p)

    with pytest.raises(NoBackendsLeftError):
        multiplexer.get(obj_id=obj_id_p)
