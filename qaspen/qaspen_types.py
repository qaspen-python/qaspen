import typing

from msgspec import Struct
from pydantic import BaseModel
from typing_extensions import Self

from qaspen.base.operators import All_, Any_

if typing.TYPE_CHECKING:
    from qaspen.table.base_table import BaseTable


class EmptyFieldValue:
    """Indicates that field wasn't queried from the database."""

    def __str__(self: Self) -> str:
        return self.__class__.__name__  # pragma: no cover


class EmptyValue:
    """Class represents that value isn't passed.

    It's necessary because `None` is valid value.
    """


EMPTY_VALUE = EmptyValue()

EMPTY_FIELD_VALUE = EmptyFieldValue()

FromTable = typing.TypeVar(
    "FromTable",
    bound="BaseTable",
)

FieldType = typing.TypeVar(
    "FieldType",
)

FieldDefaultType = typing.Union[
    FieldType,
    typing.Callable[
        [],
        FieldType,
    ],
    None,
]

CallableDefaultType = typing.Callable[
    [],
    FieldType,
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
