"""
configio
========

Async JSON/YAML config I/O with routed access.

This module is a thin, high-level façade over the per-format backends
(`jsonio` and `yamlio`) plus path navigation powered by `pyroute.Route`.
It exposes two async helpers:

- `get(path, route=None, codec=None, *, threadsafe=False) -> Optional[Any]`
    Read a document and optionally return a nested value addressed by a `Route`.
    Missing paths or recoverable parse/type errors yield `None`.

- `set(path, route=None, value=None, codec=None, *, threadsafe=False,
       overwrite_conflicts=False) -> bool`
    Read a document, apply a routed update, and persist the result using the
    corresponding backend. Returns `True` on success.

Key properties
--------------
- **Formats**: JSON and YAML. The codec may be provided explicitly or inferred
  from the file extension via `_infer_codec`.
- **Path routing**: Uses `pyroute.Route` (immutable, hashable) to address nested
  keys inside mapping-based configs.
- **Async I/O**: Files are read/written using `aiofiles`.
- **Thread offload**: Parsing/dumping can be offloaded to a worker thread when
  `threadsafe=True` to avoid blocking the event loop on large payloads.
- **Atomic saves**: Backends (`jsonio.save` / `yamlio.save`) perform best-effort
  atomic writes (temp file + `os.replace(...)`).

Return / error semantics
------------------------
- `get(...)`
    - Returns the whole document when `route` is `None`/empty.
    - Returns `None` when the route is missing or on recoverable parse/type errors.
    - **Raises** `OSError` (e.g., `FileNotFoundError`, permission issues).
- `set(...)`
    - Returns `True` on success; `False` on recoverable failures (parse/type/value).
    - **Raises** `OSError` (e.g., `FileNotFoundError`, permission issues).

Notes
-----
The traversal and mutation logic lives in `_get` / `_set` (from `configio.utils`).
By design, `_set` performs defensive updates (deep-copy first) and supports an
`overwrite_conflicts` flag to destructively convert non-mapping intermediates
into `{}` when you must reach the target route.

Examples
--------
>>> await get("config.yaml", Route("server", "port"))
>>> await set("config.json", Route("features", "beta"), True)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional, Literal

from pyroute import Route
from configio import jsonio, yamlio
from configio.utils import _infer_codec, _get, _set
from configio.schemas import PathLike, Codec

# Precise exception types for parse errors
from json import JSONDecodeError
from yaml import YAMLError


async def get(
    path: PathLike,
    route: Optional[Route] = None,
    codec: Optional[Literal[Codec.JSON, Codec.YAML]] = None,
    *,
    threadsafe: bool = False,
) -> Optional[Any]:
    """
    Read a JSON/YAML file and optionally return a nested value.

    If `route` is falsy (None or empty), the entire document is returned.
    Otherwise, the value at the given `route` is returned. When the route
    is missing or a recoverable parse/type error occurs, `None` is returned.

    Args:
        path: Filesystem path to the document (str or Path).
        route: A `Route` of hashable segments (keys). If None/empty, returns
               the full document.
        codec: Explicit codec (`Codec.JSON` or `Codec.YAML`). If omitted, it is
               inferred from the file extension via `_infer_codec`.
        threadsafe: When True, the backend parser offloads CPU-bound parsing to
                    a worker thread (recommended for large files).

    Returns:
        The nested value addressed by `route`, the entire document (when `route`
        is falsy), or `None` if the path is missing or on parse/type errors.

    Raises:
        OSError: On filesystem errors (e.g., FileNotFoundError, permission issues).
    """
    # Normalize and validate path
    if isinstance(path, str):
        p = Path(path)
    elif isinstance(path, Path):
        p = path
    else:
        raise TypeError("path must be Path or str.")

    # Infer codec once
    fmt = _infer_codec(p, codec)

    try:
        if fmt == Codec.JSON:
            data = await jsonio.load(str(p), threadsafe=threadsafe)
            return _get(data, route)
        elif fmt == Codec.YAML:
            data = await yamlio.load(str(p), threadsafe=threadsafe)
            return _get(data, route)
        # Unsupported codec → behave like "not found"
        return None
    except OSError:
        # Propagate filesystem errors so callers can distinguish them
        raise
    except (KeyError, TypeError, JSONDecodeError, YAMLError):
        # Missing route or recoverable parse/type errors
        return None


async def set(
    path: PathLike,
    route: Optional[Route] = None,
    value: Optional[Any] = None,
    codec: Optional[Literal[Codec.JSON, Codec.YAML]] = None,
    *,
    threadsafe: bool = False,
    overwrite_conflicts: bool = False,
) -> bool:
    """
    Update a JSON/YAML file at a nested route and persist the result.

    When `route` is falsy (None or empty), the entire document is replaced with
    `value`. Otherwise, `_set` writes `value` at the provided `route`, creating
    intermediate containers as needed. The appropriate backend persists the
    updated document using best-effort atomic writes.

    Args:
        path: Filesystem path to the document (str or Path).
        route: A `Route` of hashable segments (keys). If None/empty, the root
               is replaced with `value`.
        value: The value to assign at `route`.
        codec: Explicit codec (`Codec.JSON` or `Codec.YAML`). If omitted, it is
               inferred from the file extension via `_infer_codec`.
        threadsafe: When True, backend dump/parse operations are offloaded to a
                    worker thread to keep the event loop responsive.
        overwrite_conflicts: If True, `_set` will replace conflicting non-mapping
                             intermediates with `{}` to reach the target path.
                             If False (default), such conflicts raise `TypeError`.

    Returns:
        True on success; False on recoverable failures (parse/type/value).

    Raises:
        OSError: On filesystem errors (e.g., FileNotFoundError, permission issues).

    Notes:
        This function reads the current document first, applies the update via
        `_set`, and then saves it using the appropriate backend (`jsonio`/`yamlio`).
    """
    # Normalize and validate path
    if isinstance(path, str):
        p = Path(path)
    elif isinstance(path, Path):
        p = path
    else:
        raise TypeError("path must be Path or str.")

    # Infer codec once
    fmt = _infer_codec(p, codec)

    try:
        if fmt == Codec.JSON:
            data = await jsonio.load(str(p), threadsafe=threadsafe)
            new_data = _set(data, route, value, overwrite_conflicts=overwrite_conflicts)
            await jsonio.save(str(p), new_data, threadsafe=threadsafe)
            return True
        elif fmt == Codec.YAML:
            data = await yamlio.load(str(p), threadsafe=threadsafe)
            new_data = _set(data, route, value, overwrite_conflicts=overwrite_conflicts)
            await yamlio.save(str(p), new_data, threadsafe=threadsafe)
            return True
        # Unsupported codec
        return False
    except OSError:
        # Propagate so callers can decide how to handle "file not found", etc.
        raise
    except (KeyError, TypeError, ValueError, JSONDecodeError, YAMLError):
        # Any recoverable failure → consistent False
        return False
