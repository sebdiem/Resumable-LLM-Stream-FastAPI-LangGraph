import asyncio
import logging
import os
import subprocess
import sys

from psycopg import AsyncConnection


async def schema_applied() -> bool:
    async with await AsyncConnection.connect(os.getenv("DATABASE_URI", "")) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM pg_type WHERE typname = 'procrastinate_job_status'"
            )
            return await cur.fetchone() is not None


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if asyncio.run(schema_applied()):
        logging.info("Procrastinate schema already present; skipping.")
        return 0

    logging.info("Applying procrastinate schema...")
    return subprocess.run(
        ["procrastinate", "--app=src.tasks.queue.app", "schema", "--apply"]
    ).returncode


if __name__ == "__main__":
    sys.exit(main())
