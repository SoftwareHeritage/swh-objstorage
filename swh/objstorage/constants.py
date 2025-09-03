# Copyright (C) 2015-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, Literal

from swh.model.hashutil import LiteralHashAlgo

LiteralHash = LiteralHashAlgo
LiteralPrimaryHash = Literal["sha1", "sha256"]

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
