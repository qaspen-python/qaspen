from __future__ import annotations

import asyncio
import os
import sys
from typing import AsyncGenerator

import pytest
from qaspen_psycopg.engine import PsycopgEngine, PsycopgTransaction
from yarl import URL


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """Anyio backend.

    :return: backend name.
    """
    return "asyncio"


@pytest.fixture()
async def test_engine() -> AsyncGenerator[PsycopgEngine, None]:
    """Create engine and startup it."""
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(
            asyncio.WindowsSelectorEventLoopPolicy(),
        )

    db_name = os.getenv("POSTGRES_DB", "qaspendb")
    db_url = URL.build(
        scheme="postgresql",
        host="localhost",
        port=5432,
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        path=f"/{db_name}",
    )
    engine = PsycopgEngine(
        connection_url=str(db_url),
        open_connection_pool_wait=True,
        open_connection_pool_timeout=2,
    )
    await engine.create_connection_pool()
    yield engine
    await engine.stop_connection_pool()


@pytest.fixture()
async def test_db_transaction(
    test_engine: PsycopgEngine,
) -> AsyncGenerator[PsycopgTransaction, None]:
    """Create database transaction."""
    transaction = test_engine.transaction()
    yield transaction
    await transaction.rollback()
