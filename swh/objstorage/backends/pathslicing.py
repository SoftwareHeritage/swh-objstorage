# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections.abc import Iterator
from contextlib import contextmanager
from itertools import islice
import os
import random
import tempfile

from swh.model import hashutil
from swh.objstorage.exc import Error, ObjNotFoundError
from swh.objstorage.objstorage import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_LIMIT,
    ID_HASH_ALGO,
    ID_HASH_LENGTH,
    ObjStorage,
    compressors,
    compute_hash,
    decompressors,
)

BUFSIZ = 1048576

DIR_MODE = 0o755
FILE_MODE = 0o644


@contextmanager
def _write_obj_file(hex_obj_id, objstorage):
    """ Context manager for writing object files to the object storage.

    During writing, data are written to a temporary file, which is atomically
    renamed to the right file name after closing.

    Usage sample:
        with _write_obj_file(hex_obj_id, objstorage):
            f.write(obj_data)

    Yields:
        a file-like object open for writing bytes.
    """
    # Get the final paths and create the directory if absent.
    dir = objstorage._obj_dir(hex_obj_id)
    if not os.path.isdir(dir):
        os.makedirs(dir, DIR_MODE, exist_ok=True)
    path = os.path.join(dir, hex_obj_id)

    # Create a temporary file.
    (tmp, tmp_path) = tempfile.mkstemp(suffix=".tmp", prefix="hex_obj_id.", dir=dir)

    # Open the file and yield it for writing.
    tmp_f = os.fdopen(tmp, "wb")
    yield tmp_f

    # Make sure the contents of the temporary file are written to disk
    tmp_f.flush()
    if objstorage.use_fdatasync:
        os.fdatasync(tmp)
    else:
        os.fsync(tmp)

    # Then close the temporary file and move it to the right path.
    tmp_f.close()
    os.chmod(tmp_path, FILE_MODE)
    os.rename(tmp_path, path)


def _read_obj_file(hex_obj_id, objstorage):
    """ Context manager for reading object file in the object storage.

    Usage sample:
        with _read_obj_file(hex_obj_id, objstorage) as f:
            b = f.read()

    Yields:
        a file-like object open for reading bytes.
    """
    path = objstorage._obj_path(hex_obj_id)

    return open(path, "rb")


class PathSlicingObjStorage(ObjStorage):
    """Implementation of the ObjStorage API based on the hash of the content.

    On disk, an object storage is a directory tree containing files
    named after their object IDs. An object ID is a checksum of its
    content, depending on the value of the ID_HASH_ALGO constant (see
    swh.model.hashutil for its meaning).

    To avoid directories that contain too many files, the object storage has a
    given slicing. Each slicing correspond to a directory that is named
    according to the hash of its content.

    So for instance a file with SHA1 34973274ccef6ab4dfaaf86599792fa9c3fe4689
    will be stored in the given object storages :

    - 0:2/2:4/4:6 : 34/97/32/34973274ccef6ab4dfaaf86599792fa9c3fe4689
    - 0:1/0:5/    : 3/34973/34973274ccef6ab4dfaaf86599792fa9c3fe4689

    The files in the storage are stored in gzipped compressed format.

    Attributes:
        root (string): path to the root directory of the storage on the disk.
        bounds: list of tuples that indicates the beginning and the end of
            each subdirectory for a content.

    """

    def __init__(self, root, slicing, compression="gzip", **kwargs):
        """ Create an object to access a hash-slicing based object storage.

        Args:
            root (string): path to the root directory of the storage on
                the disk.
            slicing (string): string that indicates the slicing to perform
                on the hash of the content to know the path where it should
                be stored.
        """
        super().__init__(**kwargs)
        self.root = root
        # Make a list of tuples where each tuple contains the beginning
        # and the end of each slicing.
        self.bounds = [
            slice(*map(int, sbounds.split(":")))
            for sbounds in slicing.split("/")
            if sbounds
        ]

        self.use_fdatasync = hasattr(os, "fdatasync")
        self.compression = compression

        self.check_config(check_write=False)

    def check_config(self, *, check_write):
        """Check whether this object storage is properly configured"""

        root = self.root

        if not os.path.isdir(root):
            raise ValueError(
                'PathSlicingObjStorage root "%s" is not a directory' % root
            )

        max_endchar = max(map(lambda bound: bound.stop, self.bounds))
        if ID_HASH_LENGTH < max_endchar:
            raise ValueError(
                "Algorithm %s has too short hash for slicing to char %d"
                % (ID_HASH_ALGO, max_endchar)
            )

        if check_write:
            if not os.access(self.root, os.W_OK):
                raise PermissionError(
                    'PathSlicingObjStorage root "%s" is not writable' % root
                )

        if self.compression not in compressors:
            raise ValueError(
                'Unknown compression algorithm "%s" for '
                "PathSlicingObjStorage" % self.compression
            )

        return True

    def __contains__(self, obj_id):
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        return os.path.isfile(self._obj_path(hex_obj_id))

    def __iter__(self):
        """Iterate over the object identifiers currently available in the
        storage.

        Warning: with the current implementation of the object
        storage, this method will walk the filesystem to list objects,
        meaning that listing all objects will be very slow for large
        storages. You almost certainly don't want to use this method
        in production.

        Return:
            Iterator over object IDs

        """

        def obj_iterator():
            # XXX hackish: it does not verify that the depth of found files
            # matches the slicing depth of the storage
            for root, _dirs, files in os.walk(self.root):
                _dirs.sort()
                for f in sorted(files):
                    yield bytes.fromhex(f)

        return obj_iterator()

    def __len__(self):
        """Compute the number of objects available in the storage.

        Warning: this currently uses `__iter__`, its warning about bad
        performances applies

        Return:
            number of objects contained in the storage
        """
        return sum(1 for i in self)

    def _obj_dir(self, hex_obj_id):
        """ Compute the storage directory of an object.

        See also: PathSlicingObjStorage::_obj_path

        Args:
            hex_obj_id: object id as hexlified string.

        Returns:
            Path to the directory that contains the required object.
        """
        slices = [hex_obj_id[bound] for bound in self.bounds]
        return os.path.join(self.root, *slices)

    def _obj_path(self, hex_obj_id):
        """ Compute the full path to an object into the current storage.

        See also: PathSlicingObjStorage::_obj_dir

        Args:
            hex_obj_id: object id as hexlified string.

        Returns:
            Path to the actual object corresponding to the given id.
        """
        return os.path.join(self._obj_dir(hex_obj_id), hex_obj_id)

    def add(self, content, obj_id=None, check_presence=True):
        if obj_id is None:
            obj_id = compute_hash(content)
        if check_presence and obj_id in self:
            # If the object is already present, return immediately.
            return obj_id

        hex_obj_id = hashutil.hash_to_hex(obj_id)
        if not isinstance(content, Iterator):
            content = [content]
        compressor = compressors[self.compression]()
        with _write_obj_file(hex_obj_id, self) as f:
            for chunk in content:
                f.write(compressor.compress(chunk))
            f.write(compressor.flush())

        return obj_id

    def get(self, obj_id):
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)

        # Open the file and return its content as bytes
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        d = decompressors[self.compression]()
        with _read_obj_file(hex_obj_id, self) as f:
            out = d.decompress(f.read())
        if d.unused_data:
            raise Error("Corrupt object %s: trailing data found" % hex_obj_id,)

        return out

    def check(self, obj_id):
        try:
            data = self.get(obj_id)
        except OSError:
            hex_obj_id = hashutil.hash_to_hex(obj_id)
            raise Error("Corrupt object %s: not a proper compressed file" % hex_obj_id,)

        checksums = hashutil.MultiHash.from_data(
            data, hash_names=[ID_HASH_ALGO]
        ).digest()

        actual_obj_id = checksums[ID_HASH_ALGO]
        hex_obj_id = hashutil.hash_to_hex(obj_id)

        if hex_obj_id != hashutil.hash_to_hex(actual_obj_id):
            raise Error(
                "Corrupt object %s should have id %s"
                % (hashutil.hash_to_hex(obj_id), hashutil.hash_to_hex(actual_obj_id))
            )

    def delete(self, obj_id):
        super().delete(obj_id)  # Check delete permission
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)

        hex_obj_id = hashutil.hash_to_hex(obj_id)
        try:
            os.remove(self._obj_path(hex_obj_id))
        except FileNotFoundError:
            raise ObjNotFoundError(obj_id)
        return True

    # Management methods

    def get_random(self, batch_size):
        def get_random_content(self, batch_size):
            """ Get a batch of content inside a single directory.

            Returns:
                a tuple (batch size, batch).
            """
            dirs = []
            for level in range(len(self.bounds)):
                path = os.path.join(self.root, *dirs)
                dir_list = next(os.walk(path))[1]
                if "tmp" in dir_list:
                    dir_list.remove("tmp")
                dirs.append(random.choice(dir_list))

            path = os.path.join(self.root, *dirs)
            content_list = next(os.walk(path))[2]
            length = min(batch_size, len(content_list))
            return (
                length,
                map(hashutil.hash_to_bytes, random.sample(content_list, length)),
            )

        while batch_size:
            length, it = get_random_content(self, batch_size)
            batch_size = batch_size - length
            yield from it

    # Streaming methods

    @contextmanager
    def chunk_writer(self, obj_id):
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        compressor = compressors[self.compression]()
        with _write_obj_file(hex_obj_id, self) as f:
            yield lambda c: f.write(compressor.compress(c))
            f.write(compressor.flush())

    def add_stream(self, content_iter, obj_id, check_presence=True):
        if check_presence and obj_id in self:
            return obj_id

        with self.chunk_writer(obj_id) as writer:
            for chunk in content_iter:
                writer(chunk)

        return obj_id

    def get_stream(self, obj_id, chunk_size=DEFAULT_CHUNK_SIZE):
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)

        hex_obj_id = hashutil.hash_to_hex(obj_id)
        decompressor = decompressors[self.compression]()
        with _read_obj_file(hex_obj_id, self) as f:
            while True:
                raw = f.read(chunk_size)
                if not raw:
                    break
                r = decompressor.decompress(raw)
                if not r:
                    continue
                yield r

    def list_content(self, last_obj_id=None, limit=DEFAULT_LIMIT):
        if last_obj_id:
            it = self.iter_from(last_obj_id)
        else:
            it = iter(self)
        return islice(it, limit)

    def iter_from(self, obj_id, n_leaf=False):
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        slices = [hex_obj_id[bound] for bound in self.bounds]
        rlen = len(self.root.split("/"))

        i = 0
        for root, dirs, files in os.walk(self.root):
            if not dirs:
                i += 1
            level = len(root.split("/")) - rlen
            dirs.sort()
            if dirs and root == os.path.join(self.root, *slices[:level]):
                cslice = slices[level]
                for d in dirs[:]:
                    if d < cslice:
                        dirs.remove(d)
            for f in sorted(files):
                if f > hex_obj_id:
                    yield bytes.fromhex(f)
        if n_leaf:
            yield i
