from __future__ import annotations

import contextvars
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, Literal, overload

from qaspen.abc.abc_types import (
    DBConnection,
    EngineConnectionPool,
    EngineTransaction,
)
from qaspen.utils.engine_utils import parse_database

if TYPE_CHECKING:
    from typing_extensions import Self


class BaseEngine(
    ABC,
    Generic[
        DBConnection,
        EngineConnectionPool,
        EngineTransaction,
    ],
):
    """Base engine class for all possible engines."""

    engine_type: str

    def __init__(
        self: Self,
        connection_url: str,
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
        self.connection_url = connection_url

    @overload
    async def execute(  # type: ignore[overload-overlap]
        self: Self,
        querystring: str,
        querystring_parameters: list[Any],
        in_pool: bool = True,
        fetch_results: Literal[True] = True,
        **_kwargs: Any,
    ) -> list[dict[str, Any]]:
        ...

    @overload
    async def execute(
        self: Self,
        querystring: str,
        querystring_parameters: list[Any],
        in_pool: bool = True,
        fetch_results: Literal[False] = False,
        **_kwargs: Any,
    ) -> None:
        ...

    @overload
    async def execute(
        self: Self,
        querystring: str,
        querystring_parameters: list[Any],
        in_pool: bool = True,
        fetch_results: bool = True,
        **_kwargs: Any,
    ) -> list[dict[str, Any]] | None:
        ...

    @abstractmethod
    async def execute(
        self: Self,
        querystring: str,
        querystring_parameters: list[Any],
        in_pool: bool = True,
        fetch_results: bool = True,
        **_kwargs: Any,
    ) -> list[dict[str, Any]] | None:
        """Execute a querystring.

        Run querystring and return list with dict results.

        ### Parameters:
        - `querystring`: SQLable string.
        - `querystring_parameters`: parameters for querystring.
            They will be processed on driver side.
        - `in_pool`: execution in connection pool
            or in a new connection.
        - `kwargs`: just for inheritance, subclasses won't
            have problems with type hints.

        ### Returns:
        Raw result from database driver.
        """

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
    def transaction(self: Self) -> EngineTransaction:
        """Create new transaction.

        Create new transaction.
        Transaction must support async context manager protocol.

        ### Returns:
        Transaction for this engine.
        """

    @property
    def database(self: Self) -> str:
        """Get database from connection url.

        ### Returns:
        Connection from connection url.
        """
        return parse_database(self.connection_url)
