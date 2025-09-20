from __future__ import annotations

import os
import asyncio
import aiofiles
import json
from aiofiles import os as aios

from configio.utils import _random_temp
from configio.schemas import DataType, PathType


__all__ = ("load", "save")


async def load(path: PathType, threadsafe: bool = False) -> DataType:
    """
    Read a JSON file asynchronously and parse it into a Python object.

    Args:
        path: Filesystem path to the JSON file.
        threadsafe: If True, perform JSON parsing in a thread via asyncio.to_thread
                    to avoid blocking the event loop (recommended for large files).

    Returns:
        Parsed data as a Python object (dict/list/scalars), compatible with `Data`.

    Raises:
        OSError: On I/O errors.
        json.JSONDecodeError: On JSON syntax/parse errors.
    """
    async with aiofiles.open(path, "r", encoding="utf-8") as f:
        text = await f.read()

    if threadsafe:
        return await asyncio.to_thread(json.loads, text)
    return json.loads(text)


async def save(path: PathType, data: DataType, threadsafe: bool = False) -> None:
    """
    Serialize a Python object to JSON and write it to disk atomically.

    - Dump happens in a worker thread when `threadsafe=True`.
    - Write is atomic: content is written to a temp file and then
      swapped into place via `os.replace`, minimizing partial writes.

    Raises:
        OSError on I/O errors.
        TypeError if `data` contains non-JSON-serializable objects.
        ValueError for invalid numbers (NaN/Inf if allow_nan=False is set).
    """

    def _dump_str(d: DataType) -> str:
        return json.dumps(d, ensure_ascii=False, separators=(",", ":"), sort_keys=False)

    text = await asyncio.to_thread(_dump_str, data) if threadsafe else _dump_str(data)

    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        await aios.makedirs(parent, exist_ok=True)

    tmp_path = _random_temp(path)
    try:
        async with aiofiles.open(tmp_path, "w", encoding="utf-8", newline="\n") as f:
            await f.write(text)
            await asyncio.to_thread(os.fsync, f.fileno())
        await aios.replace(tmp_path, path)
    finally:
        if await aios.path.exists(tmp_path):
            try:
                await aios.remove(tmp_path)
            except Exception:
                pass
