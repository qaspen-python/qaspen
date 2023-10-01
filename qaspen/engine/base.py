import abc
import typing

from qaspen.engine.enums import DataBaseType
from qaspen.querystring.querystring import QueryString

SingleConnection = typing.TypeVar(
    "SingleConnection",
)


class BaseTransaction(abc.ABC, typing.Generic[SingleConnection]):
    def __init__(
        self: typing.Self,
        transaction_connection: SingleConnection,
    ) -> None:
        self.transaction_connection: typing.Final = transaction_connection

    @abc.abstractmethod
    async def rollback(self: typing.Self) -> None:
        """Rollback the transaction."""
        ...

    @abc.abstractmethod
    async def commit(self: typing.Self) -> None:
        """Commit the transaction."""
        ...

    @abc.abstractmethod
    async def run_query(
        self: typing.Self,
        querystring: QueryString,
    ) -> list[tuple[typing.Any, ...]]:
        ...

    @abc.abstractmethod
    async def run_query_without_result(
        self: typing.Self,
        querystring: QueryString,
    ) -> None:
        ...


ConnectionClass = typing.TypeVar(
    "ConnectionClass",
)
TransactionClass = typing.TypeVar(
    "TransactionClass",
    bound=BaseTransaction[typing.Any],
)


class BaseEngine(abc.ABC, typing.Generic[ConnectionClass, TransactionClass]):
    database_type: DataBaseType
    connection_pool: ConnectionClass | None = None

    def __init__(
        self: typing.Self,
        connection_string: str,
        **connection_parameters: typing.Any,
    ) -> None:
        self.connection_string: str = connection_string
        self.connection_parameters: typing.Any = connection_parameters

    @abc.abstractmethod
    async def startup(self: typing.Self) -> None:
        ...

    @abc.abstractmethod
    async def shutdown(self: typing.Self) -> None:
        ...

    @abc.abstractmethod
    async def transaction(
        self: typing.Self,
    ) -> TransactionClass:
        pass

    @abc.abstractmethod
    async def run_query(
        self: typing.Self,
        querystring: QueryString,
        in_transaction: bool = True,
    ) -> list[tuple[typing.Any, ...]]:
        ...

    @abc.abstractmethod
    async def run_query_without_result(
        self: typing.Self,
        querystring: QueryString,
        in_transaction: bool = True,
    ) -> None:
        ...
