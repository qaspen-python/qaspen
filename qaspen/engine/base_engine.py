import abc
import typing
from qaspen.engine.enums import DataBaseType

from qaspen.querystring.querystring import QueryString


ConnectionClass = typing.TypeVar(
    "ConnectionClass",
)


class BaseEngine(abc.ABC, typing.Generic[ConnectionClass]):

    database_type: DataBaseType
    connection: ConnectionClass | None = None

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
    async def run_query(
        self: typing.Self,
        querystring: QueryString,
        in_transaction: bool = True,
    ) -> typing.Any:
        ...
