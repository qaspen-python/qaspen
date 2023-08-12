import typing
from qaspen.exceptions import FieldComparisonError, FieldDeclarationError
from qaspen.fields import operators

from qaspen.fields.base_field import BaseField, FieldData, FieldType

from qaspen.fields.utils import validate_max_length
from qaspen.statements.combinable_statements.where_statement import (
    Where,
    WhereBetween,
)


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

    def _make_field_create_statement(
        self: typing.Self,
    ) -> str:
        return (
            f"{self._field_name} {self._build_fields_sql_type()} "
            f"{self._field_null} {self._field_default}"
        )

    def __str__(self: typing.Self) -> str:
        return str(self._field_value)

    def contains(
        self: typing.Self,
        comparison_values: typing.Iterable[typing.Any],
    ) -> Where:
        for comparison_value in comparison_values:
            is_valid_type: bool = isinstance(
                comparison_value,
                self._available_comparison_types,
            )
            if not is_valid_type:
                raise FieldComparisonError(
                    f"It's impossible to use `IN` operator "
                    f"to compare {self.__class__.__name__} "
                    f"and {type(comparison_value)}"
                )

        return Where(
            field=self,
            comparison_values=comparison_values,
            operator=operators.InOperator,
        )

    def not_contains(
        self: typing.Self,
        comparison_values: typing.Iterable[typing.Any],
    ) -> Where:
        for comparison_value in comparison_values:
            is_valid_type: bool = isinstance(
                comparison_value,
                self._available_comparison_types,
            )
            if not is_valid_type:
                raise FieldComparisonError(
                    f"It's impossible to use `NOT IN` operator "
                    f"to compare {self.__class__.__name__} "
                    f"and {type(comparison_value)}"
                )

        return Where(
            field=self,
            comparison_values=comparison_values,
            operator=operators.NotInOperator,
        )

    def between(
        self: typing.Self,
        left_value: typing.Any,
        right_value: typing.Any,
    ) -> WhereBetween:
        is_valid_type: typing.Final[bool] = all(
            (
                isinstance(
                    left_value,
                    self._available_comparison_types,
                ),
                isinstance(
                    right_value,
                    self._available_comparison_types,
                ),
            )
        )
        if is_valid_type:
            return WhereBetween(
                field=self,
                operator=operators.BetweenOperator,
                left_comparison_value=left_value,
                right_comparison_value=right_value,
            )

        raise FieldComparisonError(
            f"Incorrect type of one of the values "
            f"in `BETWEEN operator`. "
            f"You can use one of these - {self._available_comparison_types}"
        )

    def __eq__(  # type: ignore[override]
        self: typing.Self,
        comparison_value: typing.Any
    ) -> Where:
        if comparison_value is None:
            return Where(
                field=self,
                operator=operators.IsNullOperator,
            )
        if isinstance(comparison_value, self._available_comparison_types):
            return Where(
                field=self,
                comparison_value=comparison_value,
                operator=operators.EqualOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `!=` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}"
        )

    def eq(
        self: typing.Self,
        comparison_value: typing.Any
    ) -> Where:
        return self.__eq__(comparison_value)

    def __ne__(  # type: ignore[override]
        self: typing.Self,
        comparison_value: typing.Any,
    ) -> Where:
        if comparison_value is None:
            return Where(
                field=self,
                operator=operators.IsNotNullOperator,
            )
        if isinstance(comparison_value, self._available_comparison_types):
            return Where(
                field=self,
                comparison_value=comparison_value,
                operator=operators.NotEqualOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `!=` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}"
        )

    def neq(
        self: typing.Self,
        comparison_value: typing.Any
    ) -> Where:
        return self.__ne__(comparison_value)

    def __gt__(
        self: typing.Self,
        comparison_value: typing.Any,
    ) -> Where:
        if isinstance(comparison_value, self._available_comparison_types):
            return Where(
                field=self,
                comparison_value=comparison_value,
                operator=operators.GreaterOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `>` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}"
        )

    def gt(
        self: typing.Self,
        comparison_value: typing.Any,
    ) -> Where:
        return self.__gt__(comparison_value)

    def __ge__(
        self: typing.Self,
        comparison_value: typing.Any,
    ) -> Where:
        if isinstance(comparison_value, self._available_comparison_types):
            return Where(
                field=self,
                comparison_value=comparison_value,
                operator=operators.GreaterEqualOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `>=` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}"
        )

    def gte(
        self: typing.Self,
        comparison_value: typing.Any,
    ) -> Where:
        return self.__ge__(comparison_value)

    def __lt__(
        self: typing.Self,
        comparison_value: typing.Any,
    ) -> Where:
        if isinstance(comparison_value, self._available_comparison_types):
            return Where(
                field=self,
                comparison_value=comparison_value,
                operator=operators.LessOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `<` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}"
        )

    def lt(
        self: typing.Self,
        comparison_value: typing.Any,
    ) -> Where:
        return self.__lt__(comparison_value)

    def __le__(
        self: typing.Self,
        comparison_value: typing.Any,
    ) -> Where:
        if isinstance(comparison_value, self._available_comparison_types):
            return Where(
                field=self,
                comparison_value=comparison_value,
                operator=operators.LessEqualOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `<=` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}"
        )

    def lte(
        self: typing.Self,
        comparison_value: typing.Any,
    ) -> Where:
        return self.__le__(comparison_value)


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

    def like(
        self: typing.Self,
        comparison_value: str,
    ) -> Where:
        if isinstance(comparison_value, self._available_comparison_types):
            return Where(
                field=self,
                comparison_value=comparison_value,
                operator=operators.LikeOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `LIKE` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}"
        )

    def not_like(
        self: typing.Self,
        comparison_value: str,
    ) -> Where:
        if isinstance(comparison_value, self._available_comparison_types):
            return Where(
                field=self,
                comparison_value=comparison_value,
                operator=operators.NotLikeOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `NOT LIKE` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}"
        )

    def ilike(
        self: typing.Self,
        comparison_value: str,
    ) -> Where:
        if isinstance(comparison_value, self._available_comparison_types):
            return Where(
                field=self,
                comparison_value=comparison_value,
                operator=operators.ILikeOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `ILIKE` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}"
        )

    def not_ilike(
        self: typing.Self,
        comparison_value: str,
    ) -> Where:
        if isinstance(comparison_value, self._available_comparison_types):
            return Where(
                field=self,
                comparison_value=comparison_value,
                operator=operators.NotILikeOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `NOT ILIKE` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}"
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
