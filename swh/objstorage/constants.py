# Copyright (C) 2015-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, Literal

LiteralHash = Literal["sha1", "sha1_git", "sha256", "blake2s256"]
LiteralPrimaryHash = Literal["sha1", "sha256"]

ID_HASH_ALGO: Literal["sha1"] = "sha1"

ID_HEXDIGEST_LENGTH = 40
"""Size in bytes of the hash hexadecimal representation."""

ID_DIGEST_LENGTH = 20
"""Size in bytes of the hash"""

VALID_HEXCHARS = frozenset("0123456789abcdef")
"""Valid characters for hexadecimal values"""

ID_HEXDIGEST_LENGTH_BY_ALGO: Dict[LiteralPrimaryHash, int] = {
    "sha1": 40,
    "sha256": 64,
}
"""Length of a valid hexdigest for each "primary" algorithm"""


def is_valid_hexdigest(hexdigest: str, algo: LiteralPrimaryHash):
    """Return whether `hexdigest` is a valid hexdigest for the given `algo`."""
    return (
        len(hexdigest) == ID_HEXDIGEST_LENGTH_BY_ALGO[algo]
        and set(hexdigest) <= VALID_HEXCHARS
    )
