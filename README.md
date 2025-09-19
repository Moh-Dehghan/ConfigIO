# ConfigIO ⚙️
[![PyPI version](https://badge.fury.io/py/cfgio.svg)](https://pypi.org/project/cfgio/)
[![Python Versions](https://img.shields.io/pypi/pyversions/cfgio.svg)](https://pypi.org/project/cfgio/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

`configio` provides a single, loader‑driven API that can either work on files (FILE
mode) or on in‑memory Python documents (DATA mode). Nested access and updates are
addressed using [`pyroute.Route`](https://pypi.org/project/pyroute/). Parsing and
persistence are delegated to lightweight backends (`configio.jsonio`, `configio.yamlio`)
that perform best‑effort atomic writes.

---

## Features ✨

- **Two loaders, one API**: operate on disk (`loader=FILE`) or on in‑memory objects
  (`loader=DATA`) with the same methods.
- **Routed access**: immutable, hashable paths via `Route("a", "b", "c")`.
- **Async I/O**: all entry points are `async`.
- **Thread offload**: `threadsafe=True` offloads heavy parse/dump to a worker thread
  (primarily useful in FILE mode).
- **Atomic saves**: temp‑file + `os.replace(...)` pattern under the hood.
- **Strict codec**: `codec` is explicit (`Codec.JSON` or `Codec.YAML`)—no extension inference.

---

## Installation 📦

```bash
pip install cfgio -U
```

---

## Quick Start 🚀

```python
import asyncio
from configio import ConfigIO, Loader, Codec, Route
# Also you can import Route from pyroute package


async def main():
    # --- FILE mode (read from / write to disk) ---
    value = await ConfigIO.get(
        loader=Loader.FILE,
        codec=Codec.YAML,
        path="config.yml",
        route=Route("server", "port"),
    )
    print("server.port:", value)

    updated = await ConfigIO.set(
        loader=Loader.FILE,
        codec=Codec.JSON,
        path="config.json",
        route=Route("features", "beta"),
        value=True,
        save=True,               # persist to disk
        threadsafe=True,         # offload parse/dump
        overwrite_conflicts=True # create/overwrite missing/non-mapping parents as {}
    )
    print("updated FILE doc:", updated)

    # --- DATA mode (operate on an in-memory document) ---
    doc = {"app": {"theme": "light", "lang": "en"}}

    # Update in memory only
    doc = await ConfigIO.set(
        loader=Loader.DATA,
        codec=Codec.YAML,
        data=doc,
        route=Route("app", "theme"),
        value="dark",
        save=False,              # do NOT persist
    )

    # Optionally persist DATA mode to disk
    doc = await ConfigIO.set(
        loader=Loader.DATA,
        codec=Codec.YAML,
        data=doc,
        path="app.yml",
        route=Route("app", "lang"),
        value="fa-IR",
        save=True                # requires path when loader=DATA
    )

    # Delete with drop semantics
    doc = await ConfigIO.delete(
        loader=Loader.DATA,
        codec=Codec.YAML,
        data=doc,
        route=Route("app", "theme"),
        drop=True,               # prune empty parents bottom-up
        save=False
    )
    print("after delete:", doc)


asyncio.run(main())
```

---

## Routing Cheatsheet 🧭

```python
from pyroute import Route

Route("a")                # ["a"]
Route("a", "b", "c")      # ["a"]["b"]["c"]
# Use hashable keys (strings, ints...) to traverse mapping-only structures.
```

---

## Path Types 🛣️

`PathLike = Union[str, os.PathLike[str]]`  
At runtime, both plain strings and `os.PathLike` instances are accepted and validated.

---

## Example Configs 📄

**YAML**
```yaml
server:
  host: 127.0.0.1
  port: 8080
features:
  beta: false
```

**JSON**
```json
{
  "server": { "host": "127.0.0.1", "port": 8080 },
  "features": { "beta": false }
}
```

---

## License 📝

This project is licensed under the MIT License – see the [LICENSE](LICENSE) file for details.

---

## Acknowledgements 🙏

- [`pyroute`](https://pypi.org/project/pyroute/) for clean route semantics.
