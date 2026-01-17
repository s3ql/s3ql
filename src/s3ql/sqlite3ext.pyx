
# distutils: language = c++
# cython: language_level=3

from libcpp.unordered_map cimport unordered_map
from libcpp.unordered_set cimport unordered_set
from libcpp.string cimport string
from libcpp cimport bool as cpp_bool
from cython.operator cimport dereference as deref

import os


cdef extern from "_sqlite3ext.cpp":
    ctypedef int size_t
    ctypedef unordered_set[size_t] block_map_t
    cdef unordered_map[string, block_map_t] file_block_map
    cdef size_t blocksize
    cdef string vfsname

cdef extern from "<atomic>":
    cdef cppclass atomic_bool "std::atomic<bool>":
        cpp_bool load()
        void store(cpp_bool)

cdef extern from "_sqlite3ext.cpp":
    cdef atomic_bool writes_inhibited


cdef class WriteTracker:
    cdef block_map_t* block_map

    # Constructors can only accept Python objects, so we need a factory method to construct from a
    # C++ object.
    # https://cython.readthedocs.io/en/stable/src/userguide/extension_types.html#existing-pointers-instantiation
    @staticmethod
    cdef WriteTracker make(block_map_t* block_map):
        # Call to __new__ bypasses __init__ constructor
        cdef WriteTracker tracker = WriteTracker.__new__(WriteTracker)
        tracker.block_map = block_map
        return tracker

    def get_block(self):
        it = self.block_map.begin()
        if it == self.block_map.end():
            raise KeyError()
        cdef size_t blockno = deref(it)
        self.block_map.erase(it)
        return blockno

    def get_count(self):
        return self.block_map.size()

    def clear(self):
        self.block_map.clear()

def track_writes(fpath: str):
    cdef string path_c = os.fsencode(fpath)
    return WriteTracker.make(&file_block_map[path_c])

def set_blocksize(size: int):
    global blocksize
    if blocksize != 0 and blocksize != size:
        raise ValueError('block size may only be set once')
    blocksize = size

def reset():
    global blocksize
    blocksize = 0
    file_block_map.clear()

def get_vfsname():
    return vfsname.decode('utf-8')

def set_writes_inhibited(value: bool) -> None:
    """Set whether writes to tracked database files should be inhibited.

    When True, any write to the main database file will fail with SQLITE_IOERR.
    This is used during metadata upload to ensure consistent snapshots.
    """
    writes_inhibited.store(value)

def get_writes_inhibited() -> bool:
    """Return whether writes to tracked database files are currently inhibited."""
    return writes_inhibited.load()