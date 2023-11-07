from __future__ import annotations

import contextvars
import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from qaspen.abc.db_engine import BaseEngine
from qaspen.abc.db_transaction import BaseTransaction

if TYPE_CHECKING:
    from typing_extensions import Self


class PsycopgTransaction(
    BaseTransaction[
        "PsycopgEngine",
        AsyncConnection[Any],
    ],
):
    """"""

    async def __aenter__(self: Self) -> Self:
        """Enter in the async context manager.

        This method must setup new transaction.

        ### Returns:
        New transaction context manager.
        """
        self.connection = await self.retrieve_connection()
        self.transaction = self.connection.cursor()
        return self


class PsycopgEngine(
    BaseEngine[
        AsyncConnection[Any],
        AsyncConnectionPool,
        Any,
        List[Tuple[Any, ...]],
    ],
):
    """Engine for PostgreSQL based on `psycopg`."""

    def __init__(
        self: Self,
        connection_url: str,
        open_connection_pool_wait: bool | None = None,
        open_connection_pool_timeout: float | None = None,
        close_connection_pool_timeout: float | None = None,
        connection_pool_params: Dict[str, Any] | None = None,
    ) -> None:
        self.connection_url = connection_url
        self.running_transaction: contextvars.ContextVar[
            PsycopgTransaction | None,
        ] = contextvars.ContextVar(
            "running_transaction",
            default=None,
        )
        self.connection_pool_params = connection_pool_params or {}
        self.connection_pool: AsyncConnectionPool | None = None
        self.open_connection_pool_wait = open_connection_pool_wait
        self.open_connection_pool_timeout = open_connection_pool_timeout
        self.close_connection_pool_timeout = close_connection_pool_timeout

    async def prepare_database(self: Self) -> None:
        """Prepare database.

        Create necessary extensions.
        """

    async def create_connection_pool(self: Self) -> AsyncConnectionPool:
        """Create new connection pool.

        If connection pool already exists return it.

        ### Returns:
        `AsyncConnectionPool`
        """
        if not self.connection_pool:
            self.connection_pool = AsyncConnectionPool(
                conninfo=self.connection_url,
                **self.connection_pool_params,
            )
            await self.connection_pool.open(
                wait=self.open_connection_pool_wait or True,
                timeout=self.open_connection_pool_timeout or 30,
            )

        return self.connection_pool

    async def stop_connection_pool(
        self: Self,
    ) -> None:
        """Close connection pool.

        If connection pool doesn't exist, raise an error.
        """
        if not self.connection_pool:
            warnings.warn(
                "Try to close not existing connection pool.",
                stacklevel=2,
            )
            return

        await self.connection_pool.close(
            timeout=self.close_connection_pool_timeout or 30,
        )

    async def connection(self: Self) -> AsyncConnection[Any]:
        return await AsyncConnection.connect(
            conninfo=self.connection_url,
        )
