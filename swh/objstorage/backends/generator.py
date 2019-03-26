from itertools import count, islice, repeat
import random
import io
import functools
import logging

from swh.objstorage.objstorage import (
    ObjStorage, DEFAULT_CHUNK_SIZE, DEFAULT_LIMIT)

logger = logging.getLogger(__name__)


class Randomizer:
    def __init__(self):
        self.size = 0
        self.read(1024)  # create a not-so-small initial buffer

    def read(self, size):
        if size > self.size:
            with open('/dev/urandom', 'rb') as fobj:
                self.data = fobj.read(2*size)
                self.size = len(self.data)
        # pick a random subset of our existing buffer
        idx = random.randint(0, self.size - size - 1)
        return self.data[idx:idx+size]


def gen_sizes():
    '''generates numbers according to the rought distribution of file size in the
    SWH archive
    '''
    # these are the histogram bounds of the pg content.length column
    bounds = [0, 2, 72, 119, 165, 208, 256, 300, 345, 383, 429, 474, 521, 572,
              618, 676, 726, 779, 830, 879, 931, 992, 1054, 1119, 1183, 1244,
              1302, 1370, 1437, 1504, 1576, 1652, 1725, 1806, 1883, 1968, 2045,
              2133, 2236, 2338, 2433, 2552, 2659, 2774, 2905, 3049, 3190, 3322,
              3489, 3667, 3834, 4013, 4217, 4361, 4562, 4779, 5008, 5233, 5502,
              5788, 6088, 6396, 6728, 7094, 7457, 7835, 8244, 8758, 9233, 9757,
              10313, 10981, 11693, 12391, 13237, 14048, 14932, 15846, 16842,
              18051, 19487, 20949, 22595, 24337, 26590, 28840, 31604, 34653,
              37982, 41964, 46260, 51808, 58561, 66584, 78645, 95743, 122883,
              167016, 236108, 421057, 1047367, 55056238]

    nbounds = len(bounds)
    for i in count():
        idx = random.randint(1, nbounds-1)
        lower = bounds[idx-1]
        upper = bounds[idx]
        yield random.randint(lower, upper-1)


def gen_random_content(total=None, filesize=None):
    '''generates random (file) content which sizes roughly follows the SWH
    archive file size distribution (by default).

    Args:
        total (int): the total number of objects to generate. Infinite if
            unset.
        filesize (int): generate objects with fixed size instead of random
            ones.

    '''
    randomizer = Randomizer()
    if filesize:
        gen = repeat(filesize)
    else:
        gen = gen_sizes()
    if total:
        gen = islice(gen, total)
    for objsize in gen:
        yield randomizer.read(objsize)


class RandomGeneratorObjStorage(ObjStorage):
    '''A stupid read-only storage that generates blobs for testing purpose.
    '''

    def __init__(self, filesize=None, total=None, **kwargs):
        super().__init__()
        if filesize:
            filesize = int(filesize)
        self.filesize = filesize
        if total:
            total = int(total)
        self.total = total
        self._content_generator = None

    @property
    def content_generator(self):
        if self._content_generator is None:
            self._content_generator = gen_random_content(
                self.total, self.filesize)
        return self._content_generator

    def check_config(self, *, check_write):
        return True

    def __contains__(self, obj_id, *args, **kwargs):
        return False

    def __iter__(self):
        i = 1
        while True:
            j = yield (b'%d' % i)
            if self.total and i >= self.total:
                logger.debug('DONE')
                break
            if j is not None:
                i = j
            else:
                i += 1

    def get(self, obj_id, *args, **kwargs):
        return next(self.content_generator)

    def add(self, content, obj_id=None, check_presence=True, *args, **kwargs):
        pass

    def check(self, obj_id, *args, **kwargs):
        return True

    def delete(self, obj_id, *args, **kwargs):
        return True

    def get_stream(self, obj_id, chunk_size=DEFAULT_CHUNK_SIZE):
        data = io.BytesIO(next(self.content_generator))
        reader = functools.partial(data.read, chunk_size)
        yield from iter(reader, b'')

    def list_content(self, last_obj_id=None, limit=DEFAULT_LIMIT):
        it = iter(self)
        if last_obj_id:
            next(it)
            it.send(int(last_obj_id))
        return islice(it, limit)
