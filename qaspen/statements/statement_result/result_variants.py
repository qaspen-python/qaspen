import abc
import typing

ListResultType = typing.TypeVar(
    "ListResultType",
)
ObjectResultType = typing.TypeVar(
    "ObjectResultType",
)


class StatementResult(abc.ABC):
    """Base result for statement.

    Allow result to return raw result.
    As-is from engine.
    """

    @abc.abstractmethod
    def raw_result(
        self: typing.Self,
    ) -> list[tuple[typing.Any, ...]]:
        ...


class ListableStatementResult(
    abc.ABC,
    typing.Generic[ListResultType],
):
    """List result.

    Allow to return query result as a string.
    """

    @abc.abstractmethod
    def as_list(self: typing.Self) -> ListResultType:
        """Return results as a list with data."""


class ObjecttableStatementResult(
    abc.ABC,
    typing.Generic[ObjectResultType],
):
    """Object result.

    Allow to return query result as an object/list of objects.
    """

    @abc.abstractmethod
    def as_objects(self: typing.Self) -> list[ObjectResultType]:
        """Return list of objects."""
