import copy
import typing

from qaspen.base.operators import AllOperator, AnyOperator
from qaspen.base.sql_base import SQLSelectable
from qaspen.exceptions import (
    FieldComparisonError,
    FieldDeclarationError,
    FilterComparisonError,
)
from qaspen.fields import operators
from qaspen.fields.base_field import BaseField, FieldData, FieldType
from qaspen.fields.utils import validate_max_length
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.filter_statement import (
    Filter,
    FilterBetween,
)


class Field(BaseField[FieldType], SQLSelectable):
    _available_comparison_types: tuple[type, ...]

    def __init__(
        self: typing.Self,
        *pos_arguments: typing.Any,
        is_null: bool = False,
        default: FieldType | None = None,
        db_field_name: str | None = None,
    ) -> None:
        if pos_arguments:
            raise FieldDeclarationError("Use only keyword arguments.")

        if is_null and default:
            raise FieldDeclarationError(
                "It's not possible to specify is_null and default. "
                "Specify either is_null or default",
            )

        self._is_null: bool = is_null
        self._default: FieldType | None = default
        self._field_value: FieldType | None = None

        if db_field_name:
            self._field_name: str = db_field_name
        else:
            self._field_name = ""

        self._field_data: FieldData[FieldType] = FieldData(
            field_name=db_field_name if db_field_name else self._field_name,
            field_value=None,
            is_null=is_null,
            default=default,
        )

    def contains(
        self: typing.Self,
        *comparison_values: typing.Any,
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
        *comparison_values: typing.Any,
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
        left_value: typing.Any,
        right_value: typing.Any,
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
            f"{self._field_name} {self._build_fields_sql_type()} "
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

    def __str__(self: typing.Self) -> str:
        return str(self._field_value)

    def __eq__(  # type: ignore[override]
        self: typing.Self,
        comparison_value: typing.Any,
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
        comparison_value: typing.Any,
    ) -> Filter:
        return self.__eq__(comparison_value)

    def __ne__(  # type: ignore[override]
        self: typing.Self,
        comparison_value: typing.Any,
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
        comparison_value: typing.Any,
    ) -> Filter:
        return self.__ne__(comparison_value)

    def __gt__(
        self: typing.Self,
        comparison_value: typing.Any,
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
        comparison_value: typing.Any,
    ) -> Filter:
        return self.__gt__(comparison_value)

    def __ge__(
        self: typing.Self,
        comparison_value: typing.Any,
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
        comparison_value: typing.Any,
    ) -> Filter:
        return self.__ge__(comparison_value)

    def __lt__(
        self: typing.Self,
        comparison_value: typing.Any,
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
        comparison_value: typing.Any,
    ) -> Filter:
        return self.__lt__(comparison_value)

    def __le__(
        self: typing.Self,
        comparison_value: typing.Any,
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
        comparison_value: typing.Any,
    ) -> Filter:
        return self.__le__(comparison_value)


class BaseStringField(Field[str]):
    _available_comparison_types: tuple[type, ...] = (
        str,
        Field,
        AnyOperator,
        AllOperator,
    )

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

        self.max_length: typing.Final[int] = max_length if max_length else 100

    def contains(
        self: typing.Self,
        *comparison_values: str,
        subquery: SQLSelectable | None = None,
    ) -> Filter:
        return super().contains(
            *comparison_values,
            subquery=subquery,
        )

    def not_contains(
        self: typing.Self,
        *comparison_values: str,
        subquery: SQLSelectable | None = None,
    ) -> Filter:
        return super().not_contains(
            *comparison_values,
            subquery=subquery,
        )

    def between(
        self: typing.Self,
        left_value: str | Field[FieldType],
        right_value: str | Field[FieldType],
    ) -> FilterBetween:
        return super().between(left_value, right_value)

    def __eq__(  # type: ignore[override]
        self: typing.Self,
        comparison_value: (str | Field[FieldType] | AnyOperator | AllOperator),
    ) -> Filter:
        return super().__eq__(comparison_value)

    def eq(
        self: typing.Self,
        comparison_value: (str | Field[FieldType] | AnyOperator | AllOperator),
    ) -> Filter:
        return super().eq(comparison_value)

    def __ne__(  # type: ignore[override]
        self: typing.Self,
        comparison_value: str | AnyOperator | AllOperator,
    ) -> Filter:
        return super().__ne__(comparison_value)

    def neq(
        self: typing.Self,
        comparison_value: str | AnyOperator | AllOperator,
    ) -> Filter:
        return super().neq(comparison_value)

    def __gt__(
        self: typing.Self,
        comparison_value: str | AnyOperator | AllOperator,
    ) -> Filter:
        return super().__gt__(comparison_value)

    def gt(
        self: typing.Self,
        comparison_value: str | AnyOperator | AllOperator,
    ) -> Filter:
        return super().gt(comparison_value)

    def __ge__(
        self: typing.Self,
        comparison_value: str | AnyOperator | AllOperator,
    ) -> Filter:
        return super().__ge__(comparison_value)

    def gte(
        self: typing.Self,
        comparison_value: str | AnyOperator | AllOperator,
    ) -> Filter:
        return super().gte(comparison_value)

    def __lt__(
        self: typing.Self,
        comparison_value: str | AnyOperator | AllOperator,
    ) -> Filter:
        return super().__lt__(comparison_value)

    def lt(
        self: typing.Self,
        comparison_value: str | AnyOperator | AllOperator,
    ) -> Filter:
        return super().lt(comparison_value)

    def __le__(
        self: typing.Self,
        comparison_value: str | AnyOperator | AllOperator,
    ) -> Filter:
        return super().__le__(comparison_value)

    def lte(
        self: typing.Self,
        comparison_value: str | AnyOperator | AllOperator,
    ) -> Filter:
        return super().lte(comparison_value)

    def like(
        self: typing.Self,
        comparison_value: str,
    ) -> Filter:
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.LikeOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `LIKE` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )

    def not_like(
        self: typing.Self,
        comparison_value: str,
    ) -> Filter:
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.NotLikeOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `NOT LIKE` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )

    def ilike(
        self: typing.Self,
        comparison_value: str,
    ) -> Filter:
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.ILikeOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `ILIKE` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )

    def not_ilike(
        self: typing.Self,
        comparison_value: str,
    ) -> Filter:
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.NotILikeOperator,
            )
        raise FieldComparisonError(
            f"It's impossible to use `NOT ILIKE` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )

    def _build_fields_sql_type(self: typing.Self) -> str:
        return f"{self._default_field_type}({self.max_length})"

    def __set__(self: typing.Self, instance: object, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError(
                f"Can't assign not string type to {self.__class__.__name__}",
            )
        instance.__dict__[self._field_data.field_name] = value


class VarCharField(BaseStringField):
    pass


class CharField(BaseStringField):
    pass


class TextField(BaseStringField):
    _field_sql_type: typing.Literal["TEXT"] = "TEXT"

    @typing.final
    def _build_fields_sql_type(self: typing.Self) -> str:
        return self._field_sql_type
