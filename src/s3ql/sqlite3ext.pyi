# sqlite3ext.pyi
# This file provides static type hints for the 'sqlite3ext' Cython module,
# allowing type checkers like mypy to validate code that uses this extension.

# Note: The '...' notation tells mypy that the actual implementation is
# provided elsewhere (the compiled Cython module).

class WriteTracker:
    """
    Tracks which blocks of a SQLite database file have been written to.
    Instances are created via track_writes().
    """
    def get_block(self) -> int:
        """
        Retrieves and removes one block number from the set of written blocks.
        Raises KeyError if the block map is empty.
        """
        ...

    def get_count(self) -> int:
        """Returns the current number of unique written blocks being tracked."""
        ...

    def clear(self) -> None:
        """Clears the set of tracked written blocks."""
        ...

def track_writes(fpath: str) -> WriteTracker:
    """
    Begins tracking writes for the SQLite database file at 'fpath' and
    returns a WriteTracker instance bound to that file's tracking data.
    """
    ...

def set_blocksize(size: int) -> None:
    """
    Sets the global block size used by the VFS extension. Must be called
    before any file operations, and can only be set once.

    Raises ValueError if block size is already set to a different value.
    """
    ...

def reset() -> None:
    """
    Resets the VFS state, clearing the block map and block size, allowing
    a new block size to be set.
    """
    ...

def get_vfsname() -> str:
    """Returns the name under which the custom VFS is registered with SQLite."""
    ...
