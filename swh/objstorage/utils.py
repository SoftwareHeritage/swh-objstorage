# Copyright (C) 2021-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.objstorage.interface import COMPOSITE_OBJID_KEYS, HashDict


def format_obj_id(obj_id: HashDict) -> str:
    return ";".join(
        (
            "%s:%s" % (algo, obj_id[algo].hex())
            for algo in sorted(COMPOSITE_OBJID_KEYS)
            if algo in obj_id
        )
    )
