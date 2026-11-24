"""Standalone history worker: python -m cab_watch.worker."""
import asyncio
import logging
import os
import signal
import ssl

import asyncpg
from .api import apply_schema
from .stream import Stream


async def run():
    if os.getenv('TELEMETRY_MODE', 'kafka') != 'kafka':
        raise ValueError('History worker requires TELEMETRY_MODE=kafka')
    ca = os.getenv('DATABASE_SSL_CA_PEM')
    options = {'ssl': ssl.create_default_context(cadata=ca)} if ca else {}
    pool = await asyncpg.create_pool(os.environ['DATABASE_URL'], min_size=1, max_size=4, **options)
    stream = None
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed = []
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
                installed.append(sig)
            except NotImplementedError:
                pass  # asyncio.run handles Ctrl+C on Windows.
        # Same migration lock as the API; either process may start first.
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute('SELECT pg_advisory_xact_lock(73949203)')
            await apply_schema(conn)
        stream = Stream(pool, role='consumer')
        await stream.start()
        logging.getLogger(__name__).info('History worker ready: group=%s consumers=%s',stream.group,len(stream.consumers))
        await stop.wait()
    finally:
        try:
            if stream is not None:
                await stream.stop()
        finally:
            await pool.close()
            for sig in installed:
                loop.remove_signal_handler(sig)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
