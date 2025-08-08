import asyncio
import time

from .cron import run_cron_once


async def main_loop() -> None:
    while True:
        try:
            await run_cron_once()
        except Exception:
            pass
        await asyncio.sleep(300)  # every 5 minutes


if __name__ == "__main__":
    asyncio.run(main_loop())


