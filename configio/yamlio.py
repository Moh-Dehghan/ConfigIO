from __future__ import annotations

import os
import asyncio
import aiofiles
import yaml
from aiofiles import os as aios

from configio.utils import _random_temp
from configio.schemas import DataType, PathType


__all__ = ("load", "save")


# Loader
try:
    Loader = yaml.CSafeLoader
except Exception:
    Loader = yaml.SafeLoader

# Dumper
try:
    Dumper = yaml.CSafeDumper
except Exception:
    Dumper = yaml.SafeDumper


async def load(path: PathType, threadsafe: bool = False) -> DataType:
    """
    Read a YAML file asynchronously and parse it into a Python object.

    Args:
        path: Filesystem path to the YAML file.
        threadsafe: If True, perform YAML parsing in a thread via asyncio.to_thread
                    to avoid blocking the event loop (recommended for large files).

    Returns:
        Parsed data as a Python object (dict/list/scalars), compatible with `Data`.

    Raises:
        OSError: On I/O errors.
        yaml.YAMLError: On YAML syntax/parse errors.
    """
    async with aiofiles.open(path, "r", encoding="utf-8") as f:
        text = await f.read()

    if threadsafe:
        return await asyncio.to_thread(yaml.load, text, Loader=Loader)
    return yaml.load(text, Loader=Loader)


async def save(path: PathType, data: DataType, threadsafe: bool = False) -> None:
    """
    Serialize a Python object to YAML and write it to disk atomically.

    - Dump happens in a worker thread when `threadsafe=True`.
    - Write is atomic: content is written to a temp file and then
      swapped into place via `os.replace`, minimizing partial writes.

    Raises:
        OSError on I/O errors.
        yaml.YAMLError on representer/dump errors.
    """

    def _dump_str(d: DataType) -> str:
        return yaml.dump(d, Dumper=Dumper, allow_unicode=True, sort_keys=False)

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
