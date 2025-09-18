from __future__ import annotations

import os
import uuid
from typing import Any, Optional, Literal
from collections.abc import MutableMapping
from copy import deepcopy
from pathlib import Path

from pyroute import Route
from configio.schemas import PathLike, Data, Codec


def _infer_codec(
    path: Path, codec: Optional[Literal[Codec.JSON, Codec.YAML]]
) -> Literal[Codec.JSON, Codec.YAML]:
    """
    Infer the codec from the given file path if not explicitly provided.

    Priority:
      1) explicit `codec` argument
      2) file extension: .json -> JSON, .yml/.yaml -> YAML
      3) default -> JSON
    """
    if codec is not None:
        if isinstance(codec, Codec):
            return codec
        raise TypeError(f"codec ({codec}) not valid.")
    ext = path.suffix.lower()
    if ext == ".json":
        return Codec.JSON
    elif ext in (".yml", ".yaml"):
        return Codec.YAML
    raise TypeError(f"file format/codec ({ext}) not valid.")


def _random_temp(path: PathLike) -> str:
    return f"{path}.tmp.{os.getpid()}.{uuid.uuid4().hex}"


def _get(data: Data, route: Optional[Route]) -> Optional[Any]:
    """
    Traverse `data` along `route` and return the nested value.

    Args:
        data: Root mapping-like object to traverse.
        route: Successive hashable segments (keys) forming the path. If falsy
               (None or empty), the root `data` is returned as-is.

    Returns:
        The value located at the end of the route.

    Raises:
        KeyError: If any segment is missing in the current mapping.
        TypeError: If an intermediate value is not a mapping.
    """
    if not route:
        return data

    cur: Any = data
    for seg in route:
        if not isinstance(cur, MutableMapping):
            raise TypeError(
                f"cannot descend into non-mapping at segment {seg!r}: {type(cur).__name__}"
            )
        if seg not in cur:
            raise KeyError(seg)
        cur = cur[seg]
    return cur


def _set(
    data: Data,
    route: Optional[Route] = None,
    value: Optional[Any] = None,
    *,
    overwrite_conflicts: bool = False,
) -> Data:
    """
    Set a nested value inside a mapping-only structure.

    The function always deep-copies the input `data` to ensure the original
    object remains unchanged. A new root object is returned in all cases.

    Behavior:
        - If `route` is falsy (None or empty), the entire root is replaced
          with a deep copy of `value`.
        - If the copied root is `None` (e.g., from an empty YAML file),
          it is bootstrapped as an empty dict `{}`.
        - For each parent segment in the route:
            * If the key is missing: a new dict is created at that key.
            * If the key exists but its value is not a mapping:
                - Raise `TypeError` (default), OR
                - If `overwrite_conflicts=True`, replace the value with `{}`.
        - At the final segment, `value` is assigned to the key.

    Args:
        data (Data):
            The original root object (may be None, dict, or other Data).
            This object is deep-copied before modification.
        route (Optional[Route]):
            A `Route` of hashable keys specifying where to set the value.
            If None or empty, the root itself is replaced.
        value (Optional[Any]):
            The value to assign at the target route.
        overwrite_conflicts (bool, default=False):
            Whether to overwrite non-mapping values encountered on the path.
            - False: raise `TypeError` on conflicts.
            - True: destructively replace the conflicting value with `{}`.

    Returns:
        Data: A new root object (deep copy of `data` with the modification applied).

    Raises:
        TypeError:
            - If `data` is not a mapping (and not None) when a route is given.
            - If an intermediate value is not a mapping and
              `overwrite_conflicts=False`.
    """
    # Root replacement (respect "always deepcopy" semantics for consistency)
    if not route:
        return value

    root = deepcopy(data)

    # Bootstrap/validate root
    if root is None:
        cur: MutableMapping = {}
        root = cur
    else:
        if not isinstance(root, MutableMapping):
            if overwrite_conflicts:
                root = {}
                cur = root
            else:
                raise TypeError(f"expected root mapping, got {type(root).__name__}")
        else:
            cur = root

    # Walk parents: missing ⇒ {}, existing non-mapping ⇒ error or overwrite-to-{}
    for seg in route[:-1]:
        if seg in cur:
            nxt = cur[seg]
            if not isinstance(nxt, MutableMapping):
                if overwrite_conflicts:
                    nxt = {}
                    cur[seg] = nxt
                else:
                    raise TypeError(
                        f"cannot descend into non-mapping at {seg!r}: {type(nxt).__name__}"
                    )
        else:
            nxt = {}
            cur[seg] = nxt
        cur = nxt  # descend

    # Final parent must be a mapping at this point
    if not isinstance(cur, MutableMapping):
        raise TypeError(
            f"expected mapping for final parent of {route[-1]!r}, got {type(cur).__name__}"
        )

    cur[route[-1]] = value
    return root
