import asyncio
from configio import ConfigIO, Loader, Codec, Route
from pathlib import Path

from pprint import pprint


async def main():
    loader = Loader.DATA
    codec = Codec.YAML
    path = Path("test.yaml")

    # SET
    value = {"IR": "Tehran/Asia", "JP": "Tokyo/Asia", "IQ": "Baghdad/Asia"}
    data = await ConfigIO.set(
        loader,
        codec,
        data={"timezones": None},
        path=path,
        route=Route("timezones"),
        value=value,
        threadsafe=True,
        overwrite_conflicts=True,
        save=True,
    )
    pprint(data)

    # DELETE
    # result = await ConfigIO.delete(
    #     loader,
    #     codec,
    #     path=path,
    #     route=Route("NOBITEX", "endpoints", "assets", "url"),
    #     threadsafe=True,
    #     drop=False,
    #     save=True,
    # )

    # data = await ConfigIO.get(loader, codec, path=path, threadsafe=True)


if __name__ == "__main__":
    asyncio.run(main())
