from __future__ import annotations

import os
from enum import Enum
from typing import (
    Union,
    Hashable,
    MutableMapping,
    MutableSequence,
    MutableSet,
    TypeAlias,
)

# Primitive scalar values common to JSON and YAML
Scalar: TypeAlias = Union[str, int, float, bool, None]

# Keys in mappings: any hashable object (string, int, tuple, etc.)
Key: TypeAlias = Hashable

# Unified, mutable container type for both JSON and YAML
Data: TypeAlias = Union[
    Scalar,
    MutableMapping[Key, "Data"],  # e.g., dict-like, keys can be non-strings (YAML)
    MutableSequence["Data"],  # e.g., list
    MutableSet["Data"],  # e.g., set (YAML can contain sets)
]

PathLike = Union[str, os.PathLike[str]]


class Loader(Enum):
    FILE = "FILE"
    DATA = "DATA"


class Codec(Enum):
    JSON = "JSON"
    YAML = "YAML"
