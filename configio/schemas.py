from __future__ import annotations

import os
from enum import Enum
from typing import (
    Union,
    Hashable,
    MutableMapping,
    Sequence,
    AbstractSet,
    TypeAlias,
    TypeVar,
)


__all__ = ("KeyType", "DataType", "PathType", "Loader", "Codec")


KeyType = TypeVar("KeyType", bound=Hashable)

DataType: TypeAlias = Union[
    KeyType,
    MutableMapping[KeyType, "DataType[KeyType]"],
    Sequence["DataType[KeyType]"],
    AbstractSet[KeyType],
]

PathType = Union[str, os.PathLike[str]]


class Loader(Enum):
    FILE = "FILE"
    DATA = "DATA"


class Codec(Enum):
    JSON = "JSON"
    YAML = "YAML"
