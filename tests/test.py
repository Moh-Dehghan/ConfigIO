import asyncio
import configio
from configio import Route, Codec
from pathlib import Path

from pprint import pprint


async def main():
    path = Path("config.yaml")
    codec = Codec.YAML
    value = {"IR": "Tehran/Asia", "JP": "Tokyo/Asia", "IQ": "Baghdad/Asia"}
    result = await configio.set(
        path,
        Route("settings", "timezone", "tzs"),
        value,
        codec,
        threadsafe=True,
        overwrite_conflicts=False,
    )
    print("Save Success:", result)
    data = await configio.get(path, None, codec, threadsafe=True)
    pprint(data)


if __name__ == "__main__":
    asyncio.run(main())
