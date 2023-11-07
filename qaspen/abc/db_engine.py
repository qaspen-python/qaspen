from __future__ import annotations

import contextvars
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic

from qaspen.abc.abc_types import (
    DBConnection,
    EngineConnectionPool,
    EngineExecuteResult,
    EngineTransaction,
)

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.querystring.querystring import QueryString


class BaseEngine(
    ABC,
    Generic[
        DBConnection,
        EngineConnectionPool,
        EngineTransaction,
        EngineExecuteResult,
    ],
):
    """Base engine class for all possible engines."""

    def __init__(
        self: Self,
        **_kwargs: Any,
    ) -> None:
        """Initialize Engine.

        ### Parameters:
        - `kwargs`: just for inheritance, subclasses won't
            have problems with type hints.
        """
        self.running_transaction: contextvars.ContextVar[
            EngineTransaction | None,
        ] = contextvars.ContextVar(
            "running_transaction",
            default=None,
        )

    async def execute(
        self: Self,
        querystring: QueryString,
        in_pool: bool = True,
        **_kwargs: Any,
    ) -> EngineExecuteResult:
        """Execute a querystring.

        Run querystring and return raw result as in
        database driver.

        ### Parameters:
        - `querystring`: `QueryString` or it's subclasses.
        - `in_pool`: execution in connection pool
            or in a new connection.
        - `kwargs`: just for inheritance, subclasses won't
            have problems with type hints.

        ### Returns:
        Raw result from database driver.
        """
        querystring_result: EngineExecuteResult

        if in_pool:
            querystring_result = await self._execute_in_pool(
                querystring=querystring,
            )
        else:
            querystring_result = await self._execute_in_new_connection(
                querystring=querystring,
            )

        return querystring_result

    @abstractmethod
    async def prepare_database(
        self: Self,
    ) -> None:
        """Prepare database.

        Anything that must be done before database will be
        ready to execute queries.

        For example, create extensions in PostgreSQL.
        """

    @abstractmethod
    async def create_connection_pool(
        self: Self,
    ) -> EngineConnectionPool:
        """Create new connection pool.

        If connection pool already exists return it.

        ### Returns:
        Connection pool from a database driver.
        """

    @abstractmethod
    async def stop_connection_pool(
        self: Self,
    ) -> None:
        """Close connection pool.

        If connection pool doesn't exist, print warning.
        """

    @abstractmethod
    async def connection(
        self: Self,
    ) -> DBConnection:
        """Create new connection.

        Build new connection, don't take one from connection pool.

        ### Returns:
        Connection from a database driver.
        """

    @abstractmethod
    async def transaction(self: Self) -> EngineTransaction:
        """Create new transaction.

        Create new transaction.
        Transaction must support async context manager protocol.

        ### Returns:
        Transaction for this engine.
        """

    @abstractmethod
    async def _execute_in_pool(
        self: Self,
        querystring: QueryString,
    ) -> EngineExecuteResult:
        """Execute query in connection from the pool.

        Get connection from the pool and execute querystring.

        ### Parameters:
        - `querystring`: `QueryString` or it's subclasses.

        ### Returns:
        Raw result from database driver.
        """

    @abstractmethod
    async def _execute_in_new_connection(
        self: Self,
        querystring: QueryString,
    ) -> EngineExecuteResult:
        """Execute query in a new connection.

        Make new connection and execute querystring.

        ### Parameters:
        - `querystring`: `QueryString` or it's subclasses.

        ### Returns:
        Raw result from database driver.
        """
