import asyncio

from .workers import worker_loop


if __name__ == "__main__":
    asyncio.run(worker_loop())


