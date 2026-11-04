import os
import ssl
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path

import asyncpg
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from cab_watch.api import cab_runtime, router


@asynccontextmanager
async def lifespan(app):
    async with AsyncExitStack() as stack:
        pg_ca = os.getenv("DATABASE_SSL_CA_PEM")
        pg_options = {"ssl": ssl.create_default_context(cadata=pg_ca)} if pg_ca else {}
        db = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=1, max_size=4, **pg_options)
        stack.push_async_callback(db.close)
        app.state.db = db
        await stack.enter_async_context(cab_runtime(db))
        yield


app = FastAPI(title="CabWatch", lifespan=lifespan)
app.include_router(router)
assets = Path(__file__).parent / "cab_watch" / "assets"
app.mount("/cab-watch/assets", StaticFiles(directory=assets), name="assets")


@app.get("/", include_in_schema=False)
async def home():
    return RedirectResponse("/cab-watch/")

