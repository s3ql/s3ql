'''
types.py - this file is part of S3QL.

Copyright © 2026 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

from typing import Protocol


class HashFunction(Protocol):
    def update(self, data: bytes, /) -> None: ...
    def digest(self) -> bytes: ...
    def hexdigest(self) -> str: ...


class DecompressorProtocol(Protocol):
    """Protocol defining the required interface for a decompressor object."""

    def decompress(self, data: bytes, /, max_length: int = 0) -> bytes: ...

    @property
    def unused_data(self) -> bytes: ...

    @property
    def eof(self) -> bool: ...


class CompressorProtocol(Protocol):
    """Protocol defining the required interface for a compressor object."""

    def compress(self, data: bytes, /) -> bytes: ...

    def flush(self) -> bytes: ...


# Type aliases
ElementaryT = int | float | str | bytes | complex | bool | None
BasicMappingT = dict[str, ElementaryT]


class BinaryInput(Protocol):
    """Protocol for binary input streams used by backends."""

    def read(self, size: int = -1, /) -> bytes: ...
    def tell(self) -> int: ...
    def seek(self, offset: int, whence: int = 0, /) -> int: ...


class BinaryOutput(Protocol):
    """Protocol for binary output streams used by backends."""

    def write(self, data: bytes | bytearray, /) -> int: ...
    def tell(self) -> int: ...
    def seek(self, offset: int, whence: int = 0, /) -> int: ...
