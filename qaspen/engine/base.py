import abc
from typing import Any, Final, Generic, List, Optional, Tuple, TypeVar

from typing_extensions import Self

from qaspen.engine.enums import DataBaseType
from qaspen.querystring.querystring import QueryString

SingleConnection = TypeVar(
    "SingleConnection",
)


class BaseTransaction(abc.ABC, Generic[SingleConnection]):
    def __init__(
        self: Self,
        transaction_connection: SingleConnection,
    ) -> None:
        self.transaction_connection: Final = transaction_connection

    @abc.abstractmethod
    async def rollback(self: Self) -> None:
        """Rollback the transaction."""
        ...

    @abc.abstractmethod
    async def commit(self: Self) -> None:
        """Commit the transaction."""
        ...

    @abc.abstractmethod
    async def run_query(
        self: Self,
        querystring: QueryString,
    ) -> List[Tuple[Any, ...]]:
        ...

    @abc.abstractmethod
    async def run_query_without_result(
        self: Self,
        querystring: QueryString,
    ) -> None:
        ...


ConnectionClass = TypeVar(
    "ConnectionClass",
)
TransactionClass = TypeVar(
    "TransactionClass",
    bound=BaseTransaction[Any],
)


class BaseEngine(abc.ABC, Generic[ConnectionClass, TransactionClass]):
    database_type: DataBaseType
    connection_pool: Optional[ConnectionClass] = None

    def __init__(
        self: Self,
        connection_string: str,
        **connection_parameters: Any,
    ) -> None:
        self.connection_string: str = connection_string
        self.connection_parameters: Any = connection_parameters

    @abc.abstractmethod
    async def startup(self: Self) -> None:
        ...

    @abc.abstractmethod
    async def shutdown(self: Self) -> None:
        ...

    @abc.abstractmethod
    async def transaction(
        self: Self,
    ) -> TransactionClass:
        pass

    @abc.abstractmethod
    async def run_query(
        self: Self,
        querystring: QueryString,
        in_transaction: bool = True,
    ) -> List[Tuple[Any, ...]]:
        ...

    @abc.abstractmethod
    async def run_query_without_result(
        self: Self,
        querystring: QueryString,
        in_transaction: bool = True,
    ) -> None:
        ...
