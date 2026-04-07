from __future__ import annotations

import asyncio
import logging

from app.config import settings
from app.database import Database
from app.pollers import PollingCoordinator


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


async def _run() -> None:
    database = Database(settings.database_path)
    coordinator = PollingCoordinator(settings, database)
    await coordinator.start()
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await coordinator.stop()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
