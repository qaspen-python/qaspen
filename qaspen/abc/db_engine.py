from __future__ import annotations

import contextvars
import typing
from abc import ABC, abstractmethod

from qaspen.abc.abc_types import (
    DBConnection,
    EngineConnectionPool,
    EngineTransaction,
)
from qaspen.utils.engine_utils import parse_database

if typing.TYPE_CHECKING:
    from typing_extensions import Self


class BaseEngine(
    ABC,
    typing.Generic[
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
        **_kwargs: typing.Any,
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

    @typing.overload
    async def execute(  # type: ignore[misc]
        self: Self,
        querystring: str,
        in_pool: bool = True,
        fetch_results: typing.Literal[True] = True,
        **_kwargs: typing.Any,
    ) -> list[dict[str, typing.Any]]:
        ...

    @typing.overload
    async def execute(
        self: Self,
        querystring: str,
        in_pool: bool = True,
        fetch_results: typing.Literal[False] = False,
        **_kwargs: typing.Any,
    ) -> None:
        ...

    @typing.overload
    async def execute(
        self: Self,
        querystring: str,
        in_pool: bool = True,
        fetch_results: bool = True,
        **_kwargs: typing.Any,
    ) -> list[dict[str, typing.Any]] | None:
        ...

    @abstractmethod
    async def execute(
        self: Self,
        querystring: str,
        in_pool: bool = True,
        fetch_results: bool = True,
        **_kwargs: typing.Any,
    ) -> list[dict[str, typing.Any]] | None:
        """Execute a querystring.

        Run querystring and return list with dict results.

        ### Parameters:
        - `querystring`: `QueryString` or it's subclasses.
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

        typing.Anything that must be done before database will be
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
