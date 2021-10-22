# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


class Error(Exception):
    def __str__(self):
        return "storage error on object: %s" % self.args


class ObjNotFoundError(Error):
    def __str__(self):
        return "object not found: %s" % self.args


class ObjStorageAPIError(Exception):
    """ Specific internal exception of an object storage (mainly connection).
    """

    def __str__(self):
        args = self.args
        return "An unexpected error occurred in the api backend: %s" % args


class ReadOnlyObjStorage(Error):
    def __init__(self, method, *args):
        super().__init__(*args)
        self.method = method

    def __str__(self):
        return "This object storage is Read-Only: cannot use %s" % self.method


class NonIterableObjStorage(Error):
    def __init__(self, method, *args):
        super().__init__(*args)
        self.method = method

    def __str__(self):
        return "This object storage is not iterable: cannot use %s" % self.method
