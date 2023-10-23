import copy
import typing

from qaspen.base.operators import AllOperator, AnyOperator
from qaspen.base.sql_base import SQLSelectable
from qaspen.exceptions import (
    FieldComparisonError,
    FieldDeclarationError,
    FieldValueValidationError,
    FilterComparisonError,
)
from qaspen.fields import operators
from qaspen.fields.base_field import (
    BaseField,
    EmptyFieldValue,
    FieldData,
    FieldDefaultType,
    FieldType,
)
from qaspen.migrations.inheritance import ClassAsString
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.filter_statement import (
    Filter,
    FilterBetween,
)

if typing.TYPE_CHECKING:
    from qaspen.table.base_table import BaseTable

OperatorTypes = AnyOperator | AllOperator


class Field(BaseField[FieldType], SQLSelectable, ClassAsString):
    _available_comparison_types: tuple[type, ...]
    _set_available_types: tuple[type, ...]

    def __init__(
        self: typing.Self,
        *args: typing.Any,
        is_null: bool = False,
        default: FieldDefaultType[FieldType] = None,
        db_field_name: str | None = None,
    ) -> None:
        if args:
            raise FieldDeclarationError("Use only keyword arguments.")

        if is_null and default:
            raise FieldDeclarationError(
                "It's not possible to specify is_null and default. "
                "Specify either is_null or default",
            )

        self._validate_default_value(
            default_value=default,
        )

        self._field_data: FieldData[FieldType] = FieldData(
            field_name=db_field_name if db_field_name else "",
            is_null=is_null,
            default=default,
        )

    def __get__(
        self: typing.Self,
        instance: "BaseTable | None",
        owner: type["BaseTable"],
    ) -> "Field[FieldType]":
        try:
            return typing.cast(
                Field[FieldType],
                instance.__dict__[self.original_field_name],
            )
        except (AttributeError, KeyError):
            return typing.cast(
                Field[FieldType],
                owner.__dict__[self.original_field_name],
            )

    def __set__(
        self: typing.Self,
        instance: object,
        value: FieldType | EmptyFieldValue,
    ) -> None:
        field: Field[FieldType]
        if isinstance(value, EmptyFieldValue):
            field = instance.__dict__[self.original_field_name]
            field._field_data.field_value = value
            return
        elif value is None:
            field = instance.__dict__[self.original_field_name]
            field._field_data.field_value = value
            return
        else:
            if isinstance(value, self.__class__):
                instance.__dict__[self.original_field_name] = value
                return
            if not isinstance(value, self._set_available_types):
                raise TypeError(
                    f"Can't assign not string type to {self.__class__.__name__}",
                )

        self._validate_field_value(
            field_value=value,
        )
        field = instance.__dict__[self.original_field_name]
        field._field_data.field_value = value

    def contains(
        self: typing.Self,
        *comparison_values: FieldType,
        subquery: SQLSelectable | None = None,
    ) -> Filter:
        if subquery and comparison_values:
            raise FilterComparisonError(
                "It's not possible to specify subquery "
                "with positional arguments in `contains` method. "
                "Please choose either subquery or positional arguments.",
            )

        for comparison_value in comparison_values:
            is_valid_type: bool = isinstance(
                comparison_value,
                self._available_comparison_types,
            )
            if not is_valid_type:
                raise FieldComparisonError(
                    f"It's impossible to use `IN` operator "
                    f"to compare {self.__class__.__name__} "
                    f"and {type(comparison_value)}",
                )

        where_parameters: dict[str, typing.Any] = {
            "field": self,
            "operator": operators.InOperator,
        }

        if subquery:
            where_parameters["comparison_value"] = subquery
        elif comparison_values:
            where_parameters["comparison_values"] = comparison_values

        return Filter(**where_parameters)

    def not_contains(
        self: typing.Self,
        *comparison_values: FieldType,
        subquery: SQLSelectable | None = None,
    ) -> Filter:
        if subquery and comparison_values:
            raise FilterComparisonError(
                "It's not possible to specify subquery "
                "with positional arguments in `not_contains` method. "
                "Please choose either subquery or positional arguments.",
            )

        for comparison_value in comparison_values:
            is_valid_type: bool = isinstance(
                comparison_value,
                self._available_comparison_types,
            )
            if not is_valid_type:
                raise FieldComparisonError(
                    f"It's impossible to use `NOT IN` operator "
                    f"to compare {self.__class__.__name__} "
                    f"and {type(comparison_value)}",
                )

        where_parameters: dict[str, typing.Any] = {
            "field": self,
            "operator": operators.NotInOperator,
        }

        if subquery:
            where_parameters["comparison_value"] = subquery
        elif comparison_values:
            where_parameters["comparison_values"] = comparison_values

        return Filter(**where_parameters)

    def between(
        self: typing.Self,
        left_value: FieldType | "Field[typing.Any]",
        right_value: FieldType | "Field[typing.Any]",
    ) -> FilterBetween:
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
            ),
        )
        if is_valid_type:
            return FilterBetween(
                field=self,
                operator=operators.BetweenOperator,
                left_comparison_value=left_value,
                right_comparison_value=right_value,
            )

        raise FieldComparisonError(
            f"Incorrect type of one of the values "
            f"in `BETWEEN operator`. "
            f"You can use one of these - {self._available_comparison_types}",
        )

    def _make_field_create_statement(
        self: typing.Self,
    ) -> str:
        return (
            f"{self.original_field_name} {self._sql_type} "
            f"{self._field_null} {self._field_default}"
        )

    def _with_prefix(self: typing.Self, prefix: str) -> "Field[FieldType]":
        field: Field[FieldType] = copy.deepcopy(self)
        field._field_data.prefix = prefix
        return field

    def querystring(self: typing.Self) -> QueryString:
        return QueryString(
            self.field_name,
            sql_template="{}",
        )

    def __eq__(  # type: ignore[override]
        self: typing.Self,
        comparison_value: FieldType | "Field[typing.Any]" | OperatorTypes,
    ) -> Filter:
        if comparison_value is None:
            return Filter(
                field=self,
                operator=operators.IsNullOperator,
            )
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.EqualOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `!=` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )

    def eq(
        self: typing.Self,
        comparison_value: FieldType | "Field[typing.Any]" | OperatorTypes,
    ) -> Filter:
        return self.__eq__(comparison_value)

    def __ne__(  # type: ignore[override]
        self: typing.Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        if comparison_value is None:
            return Filter(
                field=self,
                operator=operators.IsNotNullOperator,
            )
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.NotEqualOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `!=` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )

    def neq(
        self: typing.Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        return self.__ne__(comparison_value)

    def __gt__(
        self: typing.Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.GreaterOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `>` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )

    def gt(
        self: typing.Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        return self.__gt__(comparison_value)

    def __ge__(
        self: typing.Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.GreaterEqualOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `>=` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )

    def gte(
        self: typing.Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        return self.__ge__(comparison_value)

    def __lt__(
        self: typing.Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.LessOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `<` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )

    def lt(
        self: typing.Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        return self.__lt__(comparison_value)

    def __le__(
        self: typing.Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.LessEqualOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `<=` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )

    def lte(
        self: typing.Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        return self.__le__(comparison_value)

    def _validate_field_value(
        self: typing.Self,
        field_value: FieldType | None,
    ) -> None:
        """Validate field value.

        If `field_value` is None but field declared as NOT NULL,
        throw an error.

        :param field_value: new value for the field.

        :raises FieldValueValidationError: if the `max_length` is exceeded.
        """
        if field_value is None and self.is_null:
            return
        if field_value is None and not self.is_null:
            raise FieldValueValidationError(
                f"You can't assign `None` to the field "
                f"that declared as `NOT NULL`",
            )

    def _validate_default_value(
        self: typing.Self,
        default_value: FieldDefaultType[FieldType],
    ) -> None:
        if default_value is None or callable(default_value):
            return
        try:
            self._validate_field_value(
                field_value=default_value,
            )
        except FieldValueValidationError as exc:
            raise FieldValueValidationError(
                f"Wrong default value in the field {self.original_field_name}",
            ) from exc

    @property
    def migration_class_name(self: typing.Self) -> str:
        return f"fields.{super().migration_class_name}"
