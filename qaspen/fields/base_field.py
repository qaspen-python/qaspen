import abc
import typing

from qaspen.exceptions import FieldDeclarationError


FieldType = typing.TypeVar(
    "FieldType",
)


class BaseField(abc.ABC):
    def __set_name__(
        self: typing.Self,
        owner: typing.Any,
        variable_name: str,
    ) -> None:
        if not self._field_name:
            self._field_name: str = variable_name

    @abc.abstractmethod
    def _make_field_create_statement(
        self: typing.Self,
    ) -> str:
        ...


class Field(BaseField, typing.Generic[FieldType]):

    _field_sql_type: str

    def __init__(
        self: typing.Self,
        *pos_arguments: typing.Any,
        field_value: FieldType | None = None,
        is_null: bool = False,
        default: FieldType | None = None,
        db_field_name: str | None = None,
    ) -> None:
        if pos_arguments:
            raise FieldDeclarationError("Use only keyword arguments.")

        if is_null and default:
            raise FieldDeclarationError(
                "It's not possible to specify is_null and default. "
                "Specify either is_null or default"
            )

        self._is_null: bool = is_null
        self._default: FieldType | None = default
        self._field_value: FieldType | None = field_value or default

        if db_field_name:
            self._field_name: str = db_field_name
        else:
            self._field_name = ""

    def __str__(self: typing.Self) -> str:
        return str(self._field_value)

    @property
    def _field_null(self: typing.Self) -> str:
        return "NOT NULL" if not self._is_null else ""

    @property
    def _field_default(self: typing.Self) -> str:
        return f"DEFAULT {self._default}" if self._default else ""

    def _build_fields_sql_type(self: typing.Self) -> str:
        return self._field_sql_type

    def _make_field_create_statement(
        self: typing.Self,
    ) -> str:
        return (
            f"{self._field_name} {self._build_fields_sql_type()} "
            f"{self._field_null} {self._field_default}"
        )
