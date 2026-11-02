import os
import ssl
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path

import asyncpg
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from cab_watch.api import CabStream, router


@asynccontextmanager
async def lifespan(app):
    async with AsyncExitStack() as stack:
        pg_ca = os.getenv("DATABASE_SSL_CA_PEM")
        pg_options = {"ssl": ssl.create_default_context(cadata=pg_ca)} if pg_ca else {}
        db = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=1, max_size=4, **pg_options)
        stack.push_async_callback(db.close)
        options = {"bootstrap_servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]}
        if os.getenv("KAFKA_SECURITY_PROTOCOL") == "SASL_SSL":
            options.update(
                security_protocol="SASL_SSL",
                ssl_context=ssl.create_default_context(cadata=os.getenv("KAFKA_SSL_CA_PEM")),
                sasl_mechanism=os.environ["KAFKA_SASL_MECHANISM"],
                sasl_plain_username=os.environ["KAFKA_USERNAME"],
                sasl_plain_password=os.environ["KAFKA_PASSWORD"],
            )
        producer = AIOKafkaProducer(**options, enable_idempotence=True)
        stack.push_async_callback(producer.stop)
        await producer.start()
        stream = CabStream(db, options)
        stack.push_async_callback(stream.stop)
        app.state.db = db
        app.state.producer = producer
        app.state.cab_stream = stream
        yield


app = FastAPI(title="CabWatch", lifespan=lifespan)
app.include_router(router)
assets = Path(__file__).parent / "cab_watch" / "assets"
app.mount("/cab-watch/assets", StaticFiles(directory=assets), name="assets")


@app.get("/", include_in_schema=False)
async def home():
    return RedirectResponse("/cab-watch/")
