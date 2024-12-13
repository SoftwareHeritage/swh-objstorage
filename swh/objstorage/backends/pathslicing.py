# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import contextmanager
from itertools import islice
import logging
import os
import tempfile
from typing import Iterator, List, Literal, Optional

from swh.objstorage.constants import (
    DEFAULT_LIMIT,
    ID_HASH_ALGO,
    ID_HEXDIGEST_LENGTH,
    is_valid_hexdigest,
)
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.interface import ObjId
from swh.objstorage.objstorage import (
    CompressionFormat,
    ObjStorage,
    compressors,
    objid_to_default_hex,
    timed,
)

BUFSIZ = 1048576

DIR_MODE = 0o755
FILE_MODE = 0o644


logger = logging.getLogger(__name__)


def is_valid_filename(filename: str, algo: Literal["sha1", "sha256"] = ID_HASH_ALGO):
    """Checks that the file points to a valid hexdigest for the given algo."""

    return is_valid_hexdigest(os.path.basename(filename), algo)


class PathSlicer:
    """Helper class to compute a path based on a hash.

    Used to compute a directory path based on the object hash according to a
    given slicing. Each slicing correspond to a directory that is named
    according to the hash of its content.

    For instance a file with SHA1 34973274ccef6ab4dfaaf86599792fa9c3fe4689
    will have the following computed path:

    - 0:2/2:4/4:6 : 34/97/32/34973274ccef6ab4dfaaf86599792fa9c3fe4689
    - 0:1/0:5/    : 3/34973/34973274ccef6ab4dfaaf86599792fa9c3fe4689

     Args:
         root (str): path to the root directory of the storage on the disk.
         slicing (str): the slicing configuration.
    """

    def __init__(self, root: str, slicing: str):
        self.root = root
        # Make a list of tuples where each tuple contains the beginning
        # and the end of each slicing.
        try:
            self.bounds = [
                slice(*(int(x) if x else None for x in sbounds.split(":")))
                for sbounds in slicing.split("/")
                if sbounds
            ]
        except TypeError:
            raise ValueError(
                "Invalid slicing declaration; "
                "it should be a of the form '<int>:<int>[/<int>:<int>]..."
            )

    def check_config(self):
        """Check the slicing configuration is valid.

        Raises:
            ValueError: if the slicing configuration is invalid.
        """
        if len(self):
            max_char = max(
                max(bound.start or 0, bound.stop or 0) for bound in self.bounds
            )
            if ID_HEXDIGEST_LENGTH < max_char:
                raise ValueError(
                    "Algorithm %s has too short hash for slicing to char %d"
                    % (ID_HASH_ALGO, max_char)
                )

    def get_directory(self, hex_obj_id: str) -> str:
        """Compute the storage directory of an object.

        See also: PathSlicer::get_path

        Args:
            hex_obj_id: object id as hexlified string.

        Returns:
            Absolute path (including root) to the directory that contains
            the given object id.
        """
        return os.path.join(self.root, *self.get_slices(hex_obj_id))

    def get_path(self, hex_obj_id: str) -> str:
        """Compute the full path to an object into the current storage.

        See also: PathSlicer::get_directory

        Args:
            hex_obj_id(str): object id as hexlified string.

        Returns:
            Absolute path (including root) to the object corresponding
            to the given object id.
        """
        return os.path.join(self.get_directory(hex_obj_id), hex_obj_id)

    def get_slices(self, hex_obj_id: str) -> List[str]:
        """Compute the path elements for the given hash.

        Args:
            hex_obj_id(str): object id as hexlified string.

        Returns:
            Relative path to the actual object corresponding to the given id as
            a list.
        """

        assert len(hex_obj_id) == ID_HEXDIGEST_LENGTH
        return [hex_obj_id[bound] for bound in self.bounds]

    def __len__(self) -> int:
        """Number of slices of the slicer"""
        return len(self.bounds)


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

    Args:
        root (str): path to the root directory of the storage on
            the disk.
        slicing (str): string that indicates the slicing to perform
            on the hash of the content to know the path where it should
            be stored (see the documentation of the PathSlicer class).

    """

    PRIMARY_HASH: Literal["sha1"] = "sha1"
    name: str = "pathslicing"

    def __init__(
        self,
        *,
        root: str = "",
        compression: CompressionFormat = "gzip",
        slicing: str = "",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.root = root
        self.slicer = PathSlicer(root, slicing)

        self.use_fdatasync = hasattr(os, "fdatasync")
        self.compression = compression

        self.check_config(check_write=False)

    def check_config(self, *, check_write):
        """Check whether this object storage is properly configured"""

        self.slicer.check_config()

        if not os.path.isdir(self.root):
            raise ValueError(
                'PathSlicingObjStorage root "%s" is not a directory' % self.root
            )

        if check_write:
            if not os.access(self.root, os.W_OK):
                raise PermissionError(
                    'PathSlicingObjStorage root "%s" is not writable' % self.root
                )

        if self.compression not in compressors:
            raise ValueError(
                'Unknown compression algorithm "%s" for '
                "PathSlicingObjStorage" % self.compression
            )

        return True

    @timed
    def __contains__(self, obj_id: ObjId) -> bool:
        hex_obj_id = objid_to_default_hex(obj_id)
        return os.path.isfile(self.slicer.get_path(hex_obj_id))

    def __iter__(self) -> Iterator[ObjId]:
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

        # XXX hackish: it does not verify that the depth of found files
        # matches the slicing depth of the storage
        for root, _dirs, files in os.walk(self.root):
            _dirs.sort()
            for f in sorted(files):
                if not is_valid_filename(f, algo=self.PRIMARY_HASH):
                    logger.warning("__iter__ skipping over invalid file %s/%s", root, f)
                    continue
                yield {self.PRIMARY_HASH: bytes.fromhex(f)}

    def __len__(self) -> int:
        """Compute the number of objects available in the storage.

        Warning: this currently uses `__iter__`, its warning about bad
        performances applies

        Return:
            number of objects contained in the storage
        """
        return sum(1 for i in self)

    @timed
    def add(
        self,
        content: bytes,
        obj_id: ObjId,
        check_presence: bool = True,
    ) -> None:
        if check_presence and obj_id in self:
            # If the object is already present, return immediately.
            return

        hex_obj_id = objid_to_default_hex(obj_id)
        with self._write_obj_file(hex_obj_id) as f:
            f.write(self.compress(content))

    @timed
    def get(self, obj_id: ObjId) -> bytes:
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)

        # Open the file and return its content as bytes
        hex_obj_id = objid_to_default_hex(obj_id)
        with open(self.slicer.get_path(hex_obj_id), "rb") as f:
            return self.decompress(f.read(), hex_obj_id)

    def delete(self, obj_id: ObjId):
        super().delete(obj_id)  # Check delete permission
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)

        hex_obj_id = objid_to_default_hex(obj_id)
        try:
            os.remove(self.slicer.get_path(hex_obj_id))
        except FileNotFoundError:
            raise ObjNotFoundError(obj_id)
        return True

    # Streaming methods

    def list_content(
        self, last_obj_id: Optional[ObjId] = None, limit: Optional[int] = DEFAULT_LIMIT
    ) -> Iterator[ObjId]:
        if last_obj_id:
            it = self.iter_from(last_obj_id)
        else:
            it = iter(self)
        return islice(it, limit)

    def iter_from(self, obj_id, n_leaf=False):
        hex_obj_id = objid_to_default_hex(obj_id)
        slices = self.slicer.get_slices(hex_obj_id)
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
                if not is_valid_filename(f, algo=self.PRIMARY_HASH):
                    logger.warning(
                        "iter_from skipping over invalid file %s/%s", root, f
                    )
                    continue
                if f > hex_obj_id:
                    yield {self.PRIMARY_HASH: bytes.fromhex(f)}
        if n_leaf:
            yield i

    @contextmanager
    def _write_obj_file(self, hex_obj_id):
        """Context manager for writing object files to the object storage.

        During writing, data are written to a temporary file, which is atomically
        renamed to the right file name after closing.

        Usage sample:
            with objstorage._write_obj_file(hex_obj_id):
                f.write(obj_data)

        Yields:
            a file-like object open for writing bytes.
        """
        # Get the final paths and create the directory if absent.
        dir = self.slicer.get_directory(hex_obj_id)
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
        if self.use_fdatasync:
            os.fdatasync(tmp)
        else:
            os.fsync(tmp)

        # Then close the temporary file and move it to the right path.
        tmp_f.close()
        os.chmod(tmp_path, FILE_MODE)
        os.rename(tmp_path, path)
