import asyncio
import logging

from src.database.checkpoint_pool import open_checkpointer
from src.tasks import queue as queue_module
from src.tasks.queue import app

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    async with open_checkpointer() as checkpointer, app.open_async():
        queue_module.checkpointer = checkpointer
        await app.run_worker_async()


if __name__ == "__main__":
    asyncio.run(main())
