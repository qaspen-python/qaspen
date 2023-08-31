import abc
import typing

from qaspen.querystring.querystring import QueryString


ConnectionClass = typing.TypeVar(
    "ConnectionClass",
)


class BaseEngine(abc.ABC, typing.Generic[ConnectionClass]):

    def __init__(
        self: typing.Self,
        connection_string: str,
    ) -> None:
        self.connection_string: str = connection_string
        self.connection: ConnectionClass | None = None

    @abc.abstractmethod
    async def create_connection(self: typing.Self) -> ConnectionClass:
        ...

    @abc.abstractmethod
    async def close_connection(self: typing.Self) -> None:
        ...

    @abc.abstractmethod
    async def run_query(
        self: typing.Self,
        querystring: QueryString,
        in_transaction: bool = True,
    ) -> typing.Any:
        ...
