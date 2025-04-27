from typing import TypeAlias, Final, Protocol
from collections.abc import Sequence
from datetime import datetime

__version__: str

class Ext:
    code: Final[int]
    data: Final[bytes]
    def __init__(self, code: int, data: bytes) -> None: ...

class Raw:
    data: Final[bytes]
    def __init__(self, data: bytes) -> None: ...

Immutable: TypeAlias = str | int | float | bool | Ext | Raw | datetime | None
Value: TypeAlias = dict[Immutable, "Value"] | Sequence["Value"] | Immutable

class Unpacker:
    def __init__(self, *, tuple: bool = False) -> None: ...
    def feed(self, data: bytes) -> None: ...
    def __iter__(self) -> "Unpacker": ...
    def __next__(self) -> Value: ...

class BinaryStream(Protocol):
    def read(self, size: int | None = -1, /) -> bytes: ...

class FileUnpacker:
    def __init__(
        self,
        file: BinaryStream,
        size: int | None = -1,
        *,
        tuple: bool = False,
    ) -> None: ...
    def __iter__(self) -> "FileUnpacker": ...
    def __next__(self) -> Value: ...

def packb(obj: Value) -> bytes: ...
def unpackb(obj: bytes, *, tuple: bool = False) -> Value: ...
