from __future__ import annotations

import os
import uuid
from typing import Any, Optional, List, Hashable
from collections.abc import MutableMapping
from copy import deepcopy

from pyroute import Route
from configio.schemas import DataType, PathType


__all__ = ("_random_temp", "_get", "_set", "_delete")


def _random_temp(path: PathType) -> str:
    return f"{path}.tmp.{os.getpid()}.{uuid.uuid4().hex}"


def _get(data: DataType, route: Optional[Route]) -> Optional[Any]:
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
    data: DataType,
    route: Optional[Route] = None,
    value: Optional[Any] = None,
    *,
    overwrite_conflicts: bool = False,
) -> DataType:
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


def _delete(
    data: DataType,
    route: Optional[Route] = None,
    *,
    drop: bool = False,
) -> DataType:
    """
    Delete semantics with copy-on-write (mirrors `_set`'s return style).

    If `route` is falsy (None or empty), the WHOLE document is deleted
    and `None` is returned.

    Modes (when `route` is non-empty):
        - drop=True  : remove key; prune empty parents bottom-up.
        - drop=False : remove subtree; if the immediate parent becomes empty,
                       replace that parent (in its own parent) with `None`.
                       Special-case: when len(route) == 1 -> root[key] = None
                       (even if key didn't exist before).

    Behavior:
        - Missing or malformed paths are silent no-ops (return unchanged copy).
        - Always deep-copies `data` and returns the modified root.
    """
    # Delete-whole-document
    if not route:
        return None

    root = deepcopy(data)

    # If root is not a mapping, partial delete is a no-op.
    if not isinstance(root, MutableMapping):
        return root

    # Top-level special case for delete-mode (non-drop, 1-segment route)
    if not drop and len(route) == 1:
        root[route[0]] = None
        return root

    # Walk to the parent of the target key
    cur = root
    parents: List[MutableMapping[Hashable, Any]] = []
    keys: List[Hashable] = []
    for seg in route[:-1]:
        if not isinstance(cur, MutableMapping) or seg not in cur:
            return root  # no-op if path missing/malformed
        parents.append(cur)
        keys.append(seg)
        cur = cur[seg]

    if not isinstance(cur, MutableMapping):
        return root  # malformed path → no-op

    target = route[-1]
    if target not in cur:
        return root  # nothing to delete

    # Remove the target subtree (or leaf)
    cur.pop(target)

    if drop:
        # Prune empty parents bottom-up
        node = cur
        for gp, k in zip(reversed(parents), reversed(keys)):
            if isinstance(node, MutableMapping) and not node:
                gp.pop(k, None)
                node = gp
            else:
                break
    else:
        # Collapse only the immediate parent to None if it became empty
        if not cur and parents:
            gp = parents[-1]
            key_of_parent = keys[-1]
            gp[key_of_parent] = None

    return root
