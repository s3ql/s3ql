from typing import Protocol


class HashFunction(Protocol):
    def update(self, data: bytes) -> None: ...
    def digest(self) -> bytes: ...
    def hexdigest(self) -> str: ...


class DecompressorProtocol(Protocol):
    """Protocol defining the required interface for a decompressor object."""

    def decompress(self, data: bytes, max_length: int = -1) -> bytes: ...

    @property
    def unused_data(self) -> bytes: ...


class CompressorProtocol(Protocol):
    """Protocol defining the required interface for a compressor object."""

    def compress(self, data: bytes) -> bytes: ...

    def flush(self) -> bytes: ...
