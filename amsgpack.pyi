from typing import TypeAlias, Final
from collections.abc import Sequence

class Ext:
    code: Final[int]
    data: Final[bytes]
    def __init__(self, code: int, data: bytes) -> None: ...

Value: TypeAlias = (
    dict[str, "Value"]
    | Sequence["Value"]
    | str
    | int
    | float
    | bool
    | Ext
    | None
)

class Unpacker:
    def feed(self, data: bytes) -> None: ...
    def __iter__(self) -> "Unpacker": ...
    def __next__(self) -> Value: ...

def packb(obj: Value) -> bytes: ...
