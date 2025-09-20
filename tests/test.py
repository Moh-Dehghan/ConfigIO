import asyncio
from configio import ConfigIO, Loader, Codec, Route

from pprint import pprint


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
        save=True,  # persist to disk
        threadsafe=True,  # offload parse/dump
        overwrite_conflicts=True,  # create/overwrite missing/non-mapping parents as {}
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
        save=False,  # do NOT persist
    )

    # Optionally persist DATA mode to disk
    doc = await ConfigIO.set(
        loader=Loader.DATA,
        codec=Codec.YAML,
        data=doc,
        path="app.yml",
        route=Route("app", "lang"),
        value="fa-IR",
        save=True,  # requires path when loader=DATA
    )

    # Delete with drop semantics
    doc = await ConfigIO.delete(
        loader=Loader.DATA,
        codec=Codec.YAML,
        data=doc,
        route=Route("app", "theme"),
        drop=True,  # prune empty parents bottom-up
        save=False,
    )
    print("after delete:", doc)


if __name__ == "__main__":
    asyncio.run(main())
