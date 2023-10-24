import abc
from typing import Any, Generic, List, Tuple, TypeVar

from typing_extensions import Self

ListResultType = TypeVar(
    "ListResultType",
)
ObjectResultType = TypeVar(
    "ObjectResultType",
)


class StatementResult(abc.ABC):
    """Base result for statement.

    Allow result to return raw result.
    As-is from engine.
    """

    @abc.abstractmethod
    def raw_result(
        self: Self,
    ) -> List[Tuple[Any, ...]]:
        ...


class ListableStatementResult(
    abc.ABC,
    Generic[ListResultType],
):
    """List result.

    Allow to return query result as a string.
    """

    @abc.abstractmethod
    def as_list(self: Self) -> ListResultType:
        """Return results as a list with data."""


class ObjecttableStatementResult(
    abc.ABC,
    Generic[ObjectResultType],
):
    """Object result.

    Allow to return query result as an object/list of objects.
    """

    @abc.abstractmethod
    def as_objects(self: Self) -> List[ObjectResultType]:
        """Return list of objects."""
