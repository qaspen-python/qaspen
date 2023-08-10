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
        self._field_data.field_name = field_name

    @abc.abstractmethod
    def _make_field_create_statement(
        self: typing.Self,
    ) -> str:
        ...

    @property
    def field_name(self: typing.Self) -> str:
        return self._field_data.field_name

    @property
    def default(self: typing.Self) -> FieldType | None:
        return self._field_data.default

    @property
    def is_null(self: typing.Self) -> bool:
        return self._field_data.is_null

    @property
    def _field_null(self: typing.Self) -> str:
        return "NOT NULL" if not self.is_null else ""

    @property
    def _field_default(self: typing.Self) -> str:
        return f"DEFAULT {self.default}" if self.default else ""

    @property
    def _default_field_type(self: typing.Self) -> str:
        return self.__class__.__name__.upper()

    def _build_fields_sql_type(self: typing.Self) -> str:
        return self._default_field_type
