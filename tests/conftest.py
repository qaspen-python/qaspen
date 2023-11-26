from __future__ import annotations

import os
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
async def test_engine() -> PsycopgEngine:
    """Create engine and startup it."""
    db_name = os.getenv("POSTGRES_DB", "qaspendb")
    db_url = URL.build(
        scheme="postgresql",
        host="localhost",
        port=5432,
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        path=f"/{db_name}",
    )
    return PsycopgEngine(
        connection_url=str(db_url),
    )


@pytest.fixture()
async def test_db_transaction(
    test_engine: PsycopgEngine,
) -> AsyncGenerator[PsycopgTransaction, None]:
    """Create database transaction."""
    transaction = test_engine.transaction()
    yield transaction
    await transaction.rollback()
