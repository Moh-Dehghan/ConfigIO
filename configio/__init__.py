"""
ConfigIO — Unified async config I/O (FILE or DATA) for JSON/YAML with routed access.

A single, loader-driven API via `ConfigIO` that works on files (FILE) or on
in-memory Python documents (DATA). Nested access/mutation is routed with
`pyroute.Route` and implemented by `_get`, `_set`, `_delete`.

Loaders
-----
FILE (`loader=Loader.FILE`)
    Operates on `path` (PathType); parses/dumps via `configio.jsonio` /
    `configio.yamlio` with best-effort atomic writes when saving.

DATA (`loader=Loader.DATA`)
    Operates directly on `data`. If you also pass `path` and set `save=True`
    in `set`/`delete`, the updated document is persisted like FILE mode.

Notes
-----
- `codec` requirements:
  * FILE mode: **required** (`Codec.JSON` or `Codec.YAML`).
  * DATA mode: **required only when persisting** (`save=True`); otherwise ignored.
- FILE mode requires `path`; DATA mode requires `data`.
- `threadsafe=True` offloads heavy parse/dump to a worker thread (relevant to FILE mode).
- Recoverable issues are logged. Some methods may return `None` in such cases even
  though the return annotation is `DataType`; treat as `DataType | None` at call sites.

Terminology
-----------
`PathType` means `Union[str, os.PathLike[str]]`. At runtime, both plain strings and
`os.PathLike` objects are accepted.
"""

from __future__ import annotations

import os
from typing import Any, Optional, Literal

from pyroute import Route
from configio import jsonio, yamlio
from configio.logger import logger
from configio.utils import _get, _set, _delete
from configio.schemas import DataType, PathType, Loader, Codec

from json import JSONDecodeError
from yaml import YAMLError


__all__ = ("ConfigIO", "Loader", "Codec", "Route")
__version__ = "1.0.1"


class ConfigIO:
    @staticmethod
    async def get(
        loader: Literal[Loader.FILE, Loader.DATA],
        *,
        codec: Optional[Literal[Codec.JSON, Codec.YAML]] = None,
        data: DataType = None,
        path: Optional[PathType] = None,
        route: Optional[Route] = None,
        threadsafe: bool = False,
    ) -> DataType:
        """
        Read a JSON/YAML document and optionally return a routed value.

        Args:
            loader:
                - `Loader.FILE`: read via `path`.
                - `Loader.DATA`: use `data` directly.
            codec:
                Required in FILE mode (`Codec.JSON` / `Codec.YAML`).
                Ignored in DATA mode.
            data:
                In-memory document (required in DATA mode).
            path:
                Filesystem path (`str | os.PathLike`; required in FILE mode).
            route:
                Nested path. If falsy (None/empty), the entire document is returned.
            threadsafe:
                Offload heavy parse to a worker thread (FILE mode).

        Returns:
            Routed value or the entire document when `route` is falsy.
            On recoverable issues this may return `None` (treat as `DataType | None`).

        Raises:
            TypeError: Missing/malformed required args for the selected mode.
            ValueError: Invalid `codec` in FILE mode.
            OSError: Filesystem errors in FILE mode.
        """
        if loader == Loader.DATA:
            try:
                return _get(data, route)
            except OSError:
                raise
            except (KeyError, TypeError) as e:
                logger.error(f"[{__name__.upper()}] Error: {e}")
        elif loader == Loader.FILE:
            if not isinstance(path, (str, os.PathLike)):
                raise TypeError("path must be str or os.PathLike")
            try:
                if codec == Codec.JSON:
                    return _get(await jsonio.load(path, threadsafe=threadsafe), route)
                elif codec == Codec.YAML:
                    return _get(await yamlio.load(path, threadsafe=threadsafe), route)
                else:
                    raise ValueError("Invalid Codec.")
            except OSError:
                raise
            except (KeyError, TypeError, JSONDecodeError, YAMLError) as e:
                logger.error(f"[{__name__.upper()}] Error: {e}")
        else:
            raise ValueError("Invalid Loader")

    @staticmethod
    async def set(
        loader: Literal[Loader.FILE, Loader.DATA],
        *,
        codec: Optional[Literal[Codec.JSON, Codec.YAML]] = None,
        data: DataType = None,
        path: Optional[PathType] = None,
        route: Optional[Route] = None,
        value: Optional[Any] = None,
        threadsafe: bool = False,
        overwrite_conflicts: bool = False,
        save: bool = True,
    ) -> DataType:
        """
        Update a document at `route` and optionally persist.

        Behavior:
        - Always returns the **updated document** (even when `save=True`).
        - DATA mode: `codec` is **only required when `save=True`** (and `path` must be set).
        - FILE mode: `codec` and `path` are required.

        Args:
            loader:
                - `Loader.FILE`: operate via `path`.
                - `Loader.DATA`: operate on `data`.
            codec:
                FILE mode: required. DATA mode: required only if `save=True`.
            data:
                In-memory document (required in DATA mode).
            path:
                Source/destination path (`str | os.PathLike`).
            route:
                Target path. If falsy, replaces the root with `value`.
            value:
                Value to assign at `route`.
            threadsafe:
                Offload heavy parse/dump (FILE mode).
            overwrite_conflicts:
                If True, non-mapping intermediates are replaced with `{}`.
            save:
                If True, persist (requires `path`; in DATA mode also a valid `codec`).

        Returns:
            Updated document on success. On recoverable issues this may return `None`
            (treat as `DataType | None`).

        Raises:
            TypeError: Missing/malformed required args for the selected mode.
            ValueError: Invalid `codec` (FILE mode, or DATA+`save=True`), or DATA+`save=True` without `path`.
            OSError: Propagated filesystem errors when persisting.
        """
        if loader == Loader.DATA:
            try:
                document = _set(
                    data, route, value, overwrite_conflicts=overwrite_conflicts
                )
                if not save:
                    return document
                # Validate Path
                if not isinstance(path, (str, os.PathLike)):
                    raise TypeError("path must be str or os.PathLike")
                # Validate Codec (required only when saving in DATA mode)
                if codec not in (Codec.JSON, Codec.YAML):
                    raise ValueError("Invalid Codec.")
                # Save
                if not await ConfigIO.save(
                    codec, document, path, threadsafe=threadsafe
                ):
                    raise OSError(
                        f"Unexpected error while saving {path} | {codec.value}"
                    )
                return document
            except (KeyError, TypeError, ValueError, JSONDecodeError, YAMLError) as e:
                logger.error(f"[{__name__.upper()}] Error: {e}")
        elif loader == Loader.FILE:
            if not isinstance(path, (str, os.PathLike)):
                raise TypeError("path must be str or os.PathLike")
            try:
                if codec == Codec.JSON:
                    document = _set(
                        await jsonio.load(path, threadsafe=threadsafe),
                        route,
                        value,
                        overwrite_conflicts=overwrite_conflicts,
                    )
                    if save:
                        if not await ConfigIO.save(
                            codec, document, path, threadsafe=threadsafe
                        ):
                            raise OSError(
                                f"Unexpected error while saving {path} | {codec.value}"
                            )
                    return document
                elif codec == Codec.YAML:
                    document = _set(
                        await yamlio.load(path, threadsafe=threadsafe),
                        route,
                        value,
                        overwrite_conflicts=overwrite_conflicts,
                    )
                    if save:
                        if not await ConfigIO.save(
                            codec, document, path, threadsafe=threadsafe
                        ):
                            raise OSError(
                                f"Unexpected error while saving {path} | {codec.value}"
                            )
                    return document
                else:
                    raise ValueError("Invalid Codec.")
            except OSError:
                raise
            except (KeyError, TypeError, ValueError, JSONDecodeError, YAMLError) as e:
                logger.error(f"[{__name__.upper()}] Error: {e}")
        else:
            raise ValueError("Invalid Loader")

    @staticmethod
    async def delete(
        loader: Literal[Loader.FILE, Loader.DATA],
        *,
        codec: Optional[Literal[Codec.JSON, Codec.YAML]] = None,
        data: DataType = None,
        path: Optional[PathType] = None,
        route: Optional[Route] = None,
        threadsafe: bool = False,
        drop: bool = False,
        save: bool = True,
    ) -> DataType:
        """
        Delete using routed semantics and optionally persist.

        Semantics (via `_delete`):
        - Falsy `route` ⇒ whole-document delete (returns `None`).
        - `drop=False` (default): remove subtree; if its immediate parent becomes empty,
          replace that parent with `None` in its parent. Special case: `len(route)==1`
          ⇒ `root[key] = None`.
        - `drop=True`: remove the key and prune empty parents bottom-up.

        Behavior:
        - Always returns the **updated document** (even when `save=True`).
        - DATA mode: `codec` is **only required when `save=True`** (and `path` must be set).
        - FILE mode: `codec` and `path` are required.

        Args:
            loader:
                - `Loader.FILE`: operate via `path`.
                - `Loader.DATA`: operate on `data`.
            codec:
                FILE mode: required. DATA mode: required only if `save=True`.
            data:
                In-memory document (required in DATA mode).
            path:
                Source/destination path (`str | os.PathLike`).
            route:
                Target path to delete.
            threadsafe:
                Offload heavy parse/dump (FILE mode).
            drop:
                Pruning policy (see semantics).
            save:
                If True, persist (requires `path`; in DATA mode also a valid `codec`).

        Returns:
            Updated document on success. On recoverable issues this may return `None`
            (treat as `DataType | None`).

        Raises:
            TypeError: Missing/malformed required args for the selected mode.
            ValueError: Invalid `codec` (FILE mode, or DATA+`save=True`), or DATA+`save=True` without `path`.
            OSError: Propagated filesystem errors when persisting.
        """
        if loader == Loader.DATA:
            try:
                document = _delete(data, route, drop=drop)
                if not save:
                    return document
                # Validate Path
                if not isinstance(path, (str, os.PathLike)):
                    raise TypeError("path must be str or os.PathLike")
                # Validate Codec (required only when saving in DATA mode)
                if codec not in (Codec.JSON, Codec.YAML):
                    raise ValueError("Invalid Codec.")
                # Save
                if not await ConfigIO.save(
                    codec, document, path, threadsafe=threadsafe
                ):
                    raise OSError(
                        f"Unexpected error while saving {path} | {codec.value}"
                    )
                return document
            except (KeyError, TypeError, ValueError, JSONDecodeError, YAMLError) as e:
                logger.error(f"[{__name__.upper()}] Error: {e}")
        elif loader == Loader.FILE:
            if not isinstance(path, (str, os.PathLike)):
                raise TypeError("path must be str or os.PathLike")
            try:
                if codec == Codec.JSON:
                    document = _delete(
                        await jsonio.load(path, threadsafe=threadsafe), route, drop=drop
                    )
                    if save:
                        if not await ConfigIO.save(
                            codec, document, path, threadsafe=threadsafe
                        ):
                            raise OSError(
                                f"Unexpected error while saving {path} | {codec.value}"
                            )
                    return document
                elif codec == Codec.YAML:
                    document = _delete(
                        await yamlio.load(path, threadsafe=threadsafe), route, drop=drop
                    )
                    if save:
                        if not await ConfigIO.save(
                            codec, document, path, threadsafe=threadsafe
                        ):
                            raise OSError(
                                f"Unexpected error while saving {path} | {codec.value}"
                            )
                    return document
                else:
                    raise ValueError("Invalid Codec.")
            except OSError:
                raise
            except (KeyError, TypeError, ValueError, JSONDecodeError, YAMLError) as e:
                logger.error(f"[{__name__.upper()}] Error: {e}")
        else:
            raise ValueError("Invalid Loader")

    @staticmethod
    async def save(
        codec: Literal[Codec.JSON, Codec.YAML],
        data: DataType,
        path: PathType,
        *,
        threadsafe: bool = False,
    ) -> bool:
        """
        Persist a document to disk using the specified `codec`.

        Args:
            codec:
                `Codec.JSON` or `Codec.YAML` (required).
            data:
                Python document to persist.
            path:
                Destination (`str | os.PathLike`).
            threadsafe:
                Offload heavy dump to a worker thread.

        Returns:
            True on success; False on recoverable serialization/logging errors.

        Raises:
            TypeError: If `path` is not `str`/`os.PathLike`.
            ValueError: Invalid `codec`.
            OSError: Propagated filesystem errors (e.g., permission issues).
        """
        if not isinstance(path, (str, os.PathLike)):
            raise TypeError("path must be str or os.PathLike")
        try:
            if codec == Codec.JSON:
                await jsonio.save(path, data, threadsafe=threadsafe)
                return True
            elif codec == Codec.YAML:
                await yamlio.save(path, data, threadsafe=threadsafe)
                return True
            else:
                raise ValueError("Invalid Codec.")
        except OSError:
            raise
        except (JSONDecodeError, YAMLError, TypeError, ValueError) as e:
            logger.error(f"[{__name__.upper()}] Error: {e}")
        return False
