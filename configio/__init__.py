"""
configio — Unified async config I/O (FILE or DATA) for JSON/YAML with routed access.

A single loader-driven API via `ConfigIO` that works on files (FILE) or on
in-memory Python documents (DATA). Nested access/mutation is routed with
`pyroute.Route` and implemented by `_get`, `_set`, `_delete`.

Modes
-----
FILE (`loader=Loader.FILE`)
    Operates on `path` (PathLike); parses/dumps via `configio.jsonio` / `configio.yamlio`
    with best-effort atomic writes when saving.

DATA (`loader=Loader.DATA`)
    Operates directly on `data`. If you pass `path` and set `save=True` in `set`/`delete`,
    the updated document is persisted like FILE mode.

Notes
-----
- `codec` is required everywhere (no extension inference here).
- FILE mode requires `path`; DATA mode requires `data`.
- `threadsafe=True` offloads heavy parse/dump to a worker thread (FILE mode).
- Recoverable issues are logged; `get` returns `None` on such cases. Filesystem `OSError`s
  are propagated.

Terminology
-----------
`PathLike` here means `Union[str, os.PathLike[str]]`; at runtime both plain strings
and `os.PathLike` objects are accepted.
"""

from __future__ import annotations

import os
from typing import Any, Optional, Literal

from pyroute import Route
from configio import jsonio, yamlio
from configio.logger import logger
from configio.utils import _get, _set, _delete
from configio.schemas import PathLike, Data, Loader, Codec

from json import JSONDecodeError
from yaml import YAMLError


__all__ = ("ConfigIO", "Loader", "Codec", "Route")
__version__ = "1.0.0"


class ConfigIO:
    @staticmethod
    async def get(
        loader: Literal[Loader.FILE, Loader.DATA],
        codec: Literal[Codec.JSON, Codec.YAML],
        *,
        data: Data = None,
        path: Optional[PathLike] = None,
        route: Optional[Route] = None,
        threadsafe: bool = False,
    ) -> Optional[Any]:
        """
        Read a JSON/YAML document and optionally return a nested value.

        Args:
            loader: FILE (read from `path`) or DATA (use `data` directly).
            codec: `Codec.JSON` or `Codec.YAML`.
            data: In-memory document (required in DATA mode).
            path: Filesystem path (PathLike = str | os.PathLike; required in FILE mode).
            route: Nested path. If falsy (None/empty), returns the whole document.
            threadsafe: Offload heavy parse to a worker thread (FILE mode).

        Returns:
            The entire document.
            Returns `None` on recoverable issues (e.g., missing route/type mismatch).

        Raises:
            TypeError: Missing/malformed required args for the selected mode.
            ValueError: Invalid `codec`.
            OSError: Filesystem errors in FILE mode.
        """
        if loader == Loader.DATA:
            try:
                if codec in (Codec.JSON, Codec.YAML):
                    return _get(data, route)
                raise ValueError("Invalid Codec.")
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
        codec: Literal[Codec.JSON, Codec.YAML],
        *,
        data: Data = None,
        path: Optional[PathLike] = None,
        route: Optional[Route] = None,
        value: Optional[Any] = None,
        threadsafe: bool = False,
        overwrite_conflicts: bool = False,
        save: bool = True,
    ) -> Data:
        """
        Update a document at `route` and optionally persist.

        Behavior:
        - Always returns the **updated document** (even when `save=True`).
        - In DATA mode with `save=True`, `path` is required and persistence mirrors FILE mode.

        Args:
            loader: FILE (operate via `path`) or DATA (operate on `data`).
            codec: `Codec.JSON` or `Codec.YAML`.
            data: In-memory document (required in DATA mode).
            path: Destination/source path (PathLike = str | os.PathLike).
            route: Nested path; if falsy, the root is replaced by `value`.
            value: Value to write at `route`.
            threadsafe: Offload heavy parse/dump (FILE mode).
            overwrite_conflicts: If True, non-mapping intermediates become `{}`.
            save: If True, persist the updated document (requires `path` in DATA mode).

        Returns:
            The updated document (`Data`) on success; `None` if a logged error occurred.

        Raises:
            TypeError: Missing/malformed required args for the selected mode.
            ValueError: Invalid `codec`, or DATA+`save=True` without `path`.
            OSError: Propagated filesystem errors when persisting.
        """
        if loader == Loader.DATA:
            try:
                document = _set(
                    data, route, value, overwrite_conflicts=overwrite_conflicts
                )
                if not save:
                    return document
                if not isinstance(path, (str, os.PathLike)):
                    raise TypeError("path must be str or os.PathLike")
                if codec in (Codec.JSON, Codec.YAML):
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
        codec: Literal[Codec.JSON, Codec.YAML],
        *,
        data: Data = None,
        path: Optional[PathLike] = None,
        route: Optional[Route] = None,
        threadsafe: bool = False,
        drop: bool = False,
        save: bool = True,
    ) -> Data:
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
        - In DATA mode with `save=True`, `path` is required and persistence mirrors FILE mode.

        Args:
            loader: FILE (operate via `path`) or DATA (operate on `data`).
            codec: `Codec.JSON` or `Codec.YAML`.
            data: In-memory document (required in DATA mode).
            path: Destination/source path (PathLike = str | os.PathLike).
            route: Target path to delete.
            threadsafe: Offload heavy parse/dump (FILE mode).
            drop: Prune mode (see semantics).
            save: If True, persist the updated document (requires `path` in DATA mode).

        Returns:
            The updated document (`Data`) on success; `None` if a logged error occurred.

        Raises:
            TypeError: Missing/malformed required args for the selected mode.
            ValueError: Invalid `codec`, or DATA+`save=True` without `path`.
            OSError: Propagated filesystem errors when persisting.
        """
        if loader == Loader.DATA:
            try:
                document = _delete(data, route, drop=drop)
                if not save:
                    return document
                if not isinstance(path, (str, os.PathLike)):
                    raise TypeError("path must be str or os.PathLike")
                if codec in (Codec.JSON, Codec.YAML):
                    if not await ConfigIO.save(
                        codec, document, path, threadsafe=threadsafe
                    ):
                        raise OSError(
                            f"Unexpected error while saving {path} | {codec.value}"
                        )
                    return document
                else:
                    raise ValueError("Invalid Codec.")
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
        data: Data,
        path: PathLike,
        *,
        threadsafe: bool = False,
    ) -> bool:
        """
        Persist a document to disk using the specified `codec`.

        Args:
            codec: `Codec.JSON` or `Codec.YAML`.
            data: Python document to persist.
            path: Destination (PathLike = str | os.PathLike).
            threadsafe: Offload heavy dump to a worker thread.

        Returns:
            True on success; False on recoverable serialization/logging errors.

        Raises:
            TypeError: If `path` is not str/os.PathLike.
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
