import typing

from msgspec import Struct
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
