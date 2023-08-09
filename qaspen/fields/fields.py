import typing
from qaspen.exceptions import FieldComparisonError, FieldDeclarationError
from qaspen.fields import operators

from qaspen.fields.base_field import BaseField, FieldData, FieldType
from qaspen.fields.comparisons import Where
from qaspen.fields.utils import validate_max_length


class Field(BaseField[FieldType]):

    _available_comparison_types: tuple[type, ...]

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

        self._field_data: FieldData[FieldType] = FieldData(
            field_name=db_field_name if db_field_name else self._field_name,
            field_value=field_value or default,
            is_null=is_null,
            default=default,
        )

    @property
    def field_name(self: typing.Self) -> str:
        return self._field_name

    @property
    def _field_null(self: typing.Self) -> str:
        return "NOT NULL" if not self._is_null else ""

    @property
    def _field_default(self: typing.Self) -> str:
        return f"DEFAULT {self._default}" if self._default else ""

    @property
    def _default_field_type(self: typing.Self) -> str:
        return self.__class__.__name__.upper()

    def _build_fields_sql_type(self: typing.Self) -> str:
        return self._default_field_type

    def _make_field_create_statement(
        self: typing.Self,
    ) -> str:
        return (
            f"{self._field_name} {self._build_fields_sql_type()} "
            f"{self._field_null} {self._field_default}"
        )

    def __str__(self: typing.Self) -> str:
        return str(self._field_value)

    def __eq__(  # type: ignore[override]
        self: typing.Self,
        comparison_value: typing.Any
    ) -> Where:
        if isinstance(comparison_value, self._available_comparison_types):
            return Where(
                field=self,
                compare_with_value=comparison_value,
                operator=operators.EqualOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `!=` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}"
        )

    def __ne__(  # type: ignore[override]
        self: typing.Self,
        comparison_value: typing.Any,
    ) -> Where:
        if isinstance(comparison_value, self._available_comparison_types):
            return Where(
                field=self,
                compare_with_value=comparison_value,
                operator=operators.NotEqualOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `=` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}"
        )

    def __gt__(
        self: typing.Self,
        comparison_value: typing.Any,
    ) -> Where:
        if isinstance(comparison_value, self._available_comparison_types):
            return Where(
                field=self,
                compare_with_value=comparison_value,
                operator=operators.GreaterOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `>` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}"
        )

    def __ge__(
        self: typing.Self,
        comparison_value: typing.Any,
    ) -> Where:
        if isinstance(comparison_value, self._available_comparison_types):
            return Where(
                field=self,
                compare_with_value=comparison_value,
                operator=operators.GreaterEqualOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `>=` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}"
        )

    def __lt__(
        self: typing.Self,
        comparison_value: typing.Any,
    ) -> Where:
        if isinstance(comparison_value, self._available_comparison_types):
            return Where(
                field=self,
                compare_with_value=comparison_value,
                operator=operators.LessOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `<` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}"
        )

    def __le__(
        self: typing.Self,
        comparison_value: typing.Any,
    ) -> Where:
        if isinstance(comparison_value, self._available_comparison_types):
            return Where(
                field=self,
                compare_with_value=comparison_value,
                operator=operators.LessEqualOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `<=` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}"
        )


class BaseStringField(Field[str]):

    _available_comparison_types: tuple[type, ...] = (str, )

    @typing.overload
    def __init__(
        self: typing.Self,
        max_length: int | None = 255,
        is_null: bool = False,
        default: str | None = None,
        db_field_name: str | None = None,
    ) -> None:
        ...

    @typing.overload
    def __init__(
        self: typing.Self,
        is_null: bool = False,
        default: str | None = None,
        db_field_name: str | None = None,
    ) -> None:
        ...

    def __init__(
        self: typing.Self,
        *pos_arguments: typing.Any,
        max_length: int | None = 255,
        is_null: bool = False,
        default: str | None = None,
        db_field_name: str | None = None,
    ) -> None:
        super().__init__(
            *pos_arguments,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )

        if max_length:
            validate_max_length(max_length=max_length)

        self.max_length: typing.Final[int] = (
            max_length if max_length else 100
        )

    def _build_fields_sql_type(self: typing.Self) -> str:
        return f"{self._default_field_type}({self.max_length})"

    def __gt__(self: typing.Self, comparison_value: str) -> Where:
        return super().__gt__(comparison_value)

    def __set__(self: typing.Self, _instance: object, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError(
                f"Can't assign not string type to {self.__class__.__name__}"
            )
        self._field_value = value


class VarCharField(BaseStringField):
    pass


class CharField(BaseStringField):
    pass


class TextField(BaseStringField):
    _field_sql_type: typing.Literal["TEXT"] = "TEXT"

    @typing.final
    def _build_fields_sql_type(self: typing.Self) -> str:
        return self._field_sql_type
