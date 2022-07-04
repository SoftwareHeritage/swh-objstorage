# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing_extensions import Literal

ID_HASH_ALGO: Literal["sha1"] = "sha1"

ID_HEXDIGEST_LENGTH = 40
"""Size in bytes of the hash hexadecimal representation."""

ID_DIGEST_LENGTH = 20
"""Size in bytes of the hash"""

DEFAULT_LIMIT = 10000
"""Default number of results of ``list_content``."""
