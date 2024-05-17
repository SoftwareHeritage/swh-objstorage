# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.core.pytest_plugin import FakeSocket
from swh.objstorage.factory import get_objstorage


@pytest.fixture
def swh_objstorage_config():
    """Fixture that returns a dictionary containing the configuration
    required to instantiate an object storage.

    Unless the fixture gets overridden, the configuration for creating an
    object storage in memory is returned.

    See :func:`swh.objstorage.factory.get_objstorage` for more details.
    """
    return {"cls": "memory"}


@pytest.fixture
def swh_objstorage(swh_objstorage_config):
    """Fixture that instantiates an object storage based on the configuration
    returned by the ``swh_objstorage_config`` fixture.
    """
    return get_objstorage(**swh_objstorage_config)


@pytest.fixture
def statsd():
    """Simple fixture giving a Statsd instance suitable for tests

    It will replace the `swh.core.statsd.statsd` instance with this one.

    The Statsd instance uses a FakeSocket as `.socket` attribute in which one
    can get the accumulated statsd messages in a deque in `.socket.payloads`.
    """

    import swh.core.statsd

    statsd = swh.core.statsd.Statsd()
    statsd._socket = FakeSocket()
    swh.core.statsd.statsd = statsd
    yield statsd
