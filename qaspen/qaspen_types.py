import typing

from pydantic import BaseModel

if typing.TYPE_CHECKING:
    from qaspen.table.base_table import BaseTable


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
        typing.Union[FieldType, typing.Any],
    ],
    None,
]

CallableDefaultType = typing.Callable[
    [],
    FieldType,
]

PydanticType = typing.TypeVar(
    "PydanticType",
    bound=BaseModel,
)
