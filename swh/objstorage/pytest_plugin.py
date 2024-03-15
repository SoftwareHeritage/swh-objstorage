# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

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
