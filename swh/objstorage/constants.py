# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict

from typing_extensions import Literal

ID_HASH_ALGO: Literal["sha1"] = "sha1"

ID_HEXDIGEST_LENGTH = 40
"""Size in bytes of the hash hexadecimal representation."""

ID_DIGEST_LENGTH = 20
"""Size in bytes of the hash"""

DEFAULT_LIMIT = 10000
"""Default number of results of ``list_content``."""

VALID_HEXCHARS = frozenset("0123456789abcdef")
"""Valid characters for hexadecimal values"""

ID_HEXDIGEST_LENGTH_BY_ALGO: Dict[Literal["sha1", "sha256"], int] = {
    "sha1": 40,
    "sha256": 64,
}
"""Length of a valid hexdigest for each "primary" algorithm"""


def is_valid_hexdigest(hexdigest: str, algo: Literal["sha1", "sha256"]):
    """Return whether `hexdigest` is a valid hexdigest for the given `algo`."""
    return (
        len(hexdigest) == ID_HEXDIGEST_LENGTH_BY_ALGO[algo]
        and set(hexdigest) <= VALID_HEXCHARS
    )
