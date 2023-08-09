import abc
import dataclasses
import typing


FieldType = typing.TypeVar(
    "FieldType",
)


@dataclasses.dataclass
class FieldData(typing.Generic[FieldType]):
    field_name: str
    is_null: bool = False
    field_value: FieldType | None = None
    default: FieldType | None = None


class BaseField(abc.ABC, typing.Generic[FieldType]):

    _field_data: FieldData[FieldType]

    def __set_name__(
        self: typing.Self,
        owner: typing.Any,
        field_name: str,
    ) -> None:
        if not self._field_name:
            self._field_name: str = field_name

    @abc.abstractmethod
    def _make_field_create_statement(
        self: typing.Self,
    ) -> str:
        ...
