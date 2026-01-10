from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol, TypeVar

if TYPE_CHECKING:
    from s3ql.backends.comprenc import ComprencBackend


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


# Type variables
T = TypeVar('T')

# Type aliases
ElementaryT = int | float | str | bytes | complex | bool | None
BasicMappingT = dict[str, ElementaryT]


class BackendFactory(Protocol):
    """Protocol for backend factory functions."""

    def __call__(self) -> ComprencBackend: ...

    has_delete_multi: bool


class BackendOptionsProtocol(Protocol):
    """Protocol for backend options object passed to backend constructors."""

    backend_class: type
    storage_url: str
    backend_options: dict[str, str | bool]
    backend_login: str
    backend_password: str


class BinaryInput(Protocol):
    """Protocol for binary input streams used by backends."""

    def read(self, size: int = -1, /) -> bytes: ...
    def tell(self) -> int: ...
    def seek(self, offset: int, whence: int = 0, /) -> int: ...


class BinaryOutput(Protocol):
    """Protocol for binary output streams used by backends."""

    def write(self, data: bytes, /) -> int: ...
    def tell(self) -> int: ...
    def seek(self, offset: int, whence: int = 0, /) -> int: ...


# Type alias for functions decorated with handle_on_return
OnReturnFn = Callable[..., T]
