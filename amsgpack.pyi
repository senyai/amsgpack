from typing import (
    TypeAlias,
    Final,
    Protocol,
    Callable,
    final,
    TypeVar,
    Generic,
    Sequence,
    Mapping,
)
from datetime import datetime

__version__: str

class ComparableAndHashable(Protocol):
    def __eq__(self, value: object) -> bool: ...
    def __ge__(self, value: object) -> bool: ...
    def __gt__(self, value: object) -> bool: ...
    def __le__(self, value: object) -> bool: ...
    def __lt__(self, value: object) -> bool: ...
    def __ne__(self, value: object) -> bool: ...
    def __hash__(self) -> int: ...

@final
class Timestamp(ComparableAndHashable):
    def __init__(self, seconds: int, nanoseconds: int = 0) -> None: ...
    seconds: Final[int]
    nanoseconds: Final[int]

@final
class Ext:
    code: Final[int]
    data: Final[bytes]
    def __init__(self, code: int, data: bytes) -> None: ...
    def is_timestamp(self) -> bool: ...
    def default(self) -> Ext | datetime: ...
    def to_timestamp(self) -> Timestamp: ...
    def to_datetime(self) -> datetime: ...
    def __hash__(self) -> int: ...
    def __eq__(self, value: object) -> bool: ...
    def __ne__(self, value: object) -> bool: ...

@final
class Raw(ComparableAndHashable):
    data: Final[bytes]
    def __init__(self, data: bytes) -> None: ...

Immutable: TypeAlias = (
    str | int | float | bool | bytes | Ext | Raw | datetime | Timestamp | None
)
Value: TypeAlias = (
    Mapping[str, Value]
    | Mapping[Immutable, Value]
    | Sequence[Value]
    | Immutable
    | bytearray
)

TP = TypeVar("TP", default=Value)

@final
class Packer(Generic[TP]):
    def __init__(
        self, default: Callable[[TP], Value] | None = None
    ) -> None: ...
    def packb(self, obj: Value | TP) -> bytes: ...

TU = TypeVar("TU", default=Ext)

@final
class Unpacker(Generic[TU]):
    def __init__(
        self,
        *,
        tuple: bool = False,
        ext_hook: Callable[[Ext], TU] | None = None,
    ) -> None: ...
    def feed(self, data: bytes) -> None: ...
    def reset(self) -> None: ...
    def unpackb(self, obj: bytes | memoryview) -> Value | TU: ...
    def __iter__(self) -> "Unpacker": ...
    def __next__(self) -> Value | TU: ...

class BinaryStream(Protocol):
    def read(self, size: int | None = -1, /) -> bytes: ...

@final
class FileUnpacker(Generic[TU]):
    def __init__(
        self,
        file: BinaryStream,
        size: int | None = -1,
        *,
        tuple: bool = False,
        ext_hook: Callable[[Ext], TU] | None = None,
    ) -> None: ...
    def __iter__(self) -> FileUnpacker[TU]: ...
    def __next__(self) -> Value | TU: ...

packb = Packer().packb
unpackb = Unpacker().unpackb
