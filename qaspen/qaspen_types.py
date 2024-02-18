import typing

from msgspec import Struct
from pydantic import BaseModel
from typing_extensions import Self

from qaspen.base.operators import All_, Any_

if typing.TYPE_CHECKING:
    from qaspen.table.base_table import BaseTable


class EmptyColumnValue:
    """Indicates that column wasn't queried from the database."""

    def __str__(self: Self) -> str:
        return self.__class__.__name__  # pragma: no cover


class EmptyValue:
    """Class represents that value isn't passed.

    It's necessary because `None` is valid value.
    """


EMPTY_VALUE = EmptyValue()

EMPTY_FIELD_VALUE = EmptyColumnValue()

FromTable = typing.TypeVar(
    "FromTable",
    bound="BaseTable",
)

ColumnType = typing.TypeVar(
    "ColumnType",
)

ColumnDefaultType = typing.Union[
    ColumnType,
    typing.Callable[
        [],
        ColumnType,
    ],
    None,
]

CallableDefaultType = typing.Callable[
    [],
    ColumnType,
]

PydanticModel = typing.TypeVar(
    "PydanticModel",
    bound=BaseModel,
)

MSGSpecStruct = typing.TypeVar(
    "MSGSpecStruct",
    bound=Struct,
)

OperatorTypes = typing.Union[Any_, All_]

ComparisonT = typing.TypeVar(
    "ComparisonT",
)
