# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
from deprecated import deprecated


def _exc_message(base: str, args):
    message = base
    if args:
        message += f": {args[0]}"
    return message


class Error(Exception):
    def __str__(self):
        return _exc_message("Object storage error on object", self.args)


class ObjNotFoundError(Error):
    def __str__(self):
        return _exc_message("Object not found", self.args)


class ObjCorruptedError(Error):
    def __str__(self) -> str:
        return _exc_message("Object corrupted", self.args)


class ObjStorageAPIError(Exception):
    """Specific internal exception of an object storage (mainly connection)."""

    def __str__(self):
        return _exc_message(
            "An unexpected error occurred in the api backend", self.args
        )


class ReadOnlyObjStorageError(Error):
    def __init__(self, method, *args):
        super().__init__(*args)
        self.method = method

    def __str__(self):
        return f"This object storage is Read-Only: cannot use {self.method}"


ReadOnlyObjStorage = deprecated(
    version="v3.1.0",
    reason="This exception has been renamed as ReadOnlyObjStorageError",
)(ReadOnlyObjStorageError)


class NonIterableObjStorageError(Error):
    def __init__(self, method, *args):
        super().__init__(*args)
        self.method = method

    def __str__(self):
        return f"This object storage is not iterable: cannot use {self.method}"


NonIterableObjStorage = deprecated(
    version="v3.1.0",
    reason="This exception has been renamed as NonIterableObjStorageError",
)(NonIterableObjStorageError)
