import copy
import types
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Final,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

from typing_extensions import Self

from qaspen.base.operators import AllOperator, AnyOperator
from qaspen.base.sql_base import SQLSelectable
from qaspen.exceptions import (
    FieldComparisonError,
    FieldDeclarationError,
    FieldValueValidationError,
    FilterComparisonError,
)
from qaspen.fields import operators
from qaspen.fields.base_field import BaseField, EmptyFieldValue, FieldData
from qaspen.qaspen_types import FieldDefaultType, FieldType
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.filter_statement import (
    Filter,
    FilterBetween,
)

if TYPE_CHECKING:
    from qaspen.table.base_table import BaseTable

OperatorTypes = Union[AnyOperator, AllOperator]


class Field(BaseField[FieldType], SQLSelectable):
    _available_comparison_types: Tuple[type, ...]
    _set_available_types: Tuple[type, ...]

    def __init__(
        self: Self,
        *args: Any,
        is_null: bool = False,
        default: FieldDefaultType[FieldType] = None,
        db_field_name: Optional[str] = None,
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
            field_name=db_field_name or "",
            is_null=is_null,
            default=default,
        )

    def __get__(
        self: Self,
        instance: "Optional[BaseTable]",
        owner: "Type[BaseTable]",
    ) -> "Field[FieldType]":
        try:
            return cast(
                Field[FieldType],
                instance.__dict__[self._original_field_name],
            )
        except (AttributeError, KeyError):
            return cast(
                Field[FieldType],
                owner.__dict__[self._original_field_name],
            )

    def __set__(
        self: Self,
        instance: object,
        value: Optional[Union[FieldType, EmptyFieldValue]],
    ) -> None:
        field: Field[FieldType]
        if isinstance(value, EmptyFieldValue) or value is None:
            field = instance.__dict__[self._original_field_name]
            field._field_data.field_value = value
            return

        if isinstance(value, self.__class__):
            instance.__dict__[self._original_field_name] = value
            return

        if not isinstance(value, self._set_available_types):
            raise TypeError(
                f"Can't assign not {self._field_type} "
                f"type to {self.__class__.__name__}",
            )

        self._validate_field_value(
            field_value=value,  # type: ignore[arg-type]
        )
        field = instance.__dict__[self._original_field_name]
        field._field_data.field_value = value  # type: ignore[assignment]

    def _prepare_default_value(self: Self) -> FieldDefaultType[FieldType]:
        """Prepare default value to specify it in Field declaration.

        It uses only in the method `_field_default`.

        :returns: prepared default value.
        """
        return self._default

    def in_(
        self: Self,
        *comparison_values: FieldType,
        subquery: Optional[SQLSelectable] = None,
    ) -> Filter:
        if subquery and comparison_values:
            raise FilterComparisonError(
                "It's not possible to specify subquery "
                "with positional arguments in `in_` method. "
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

        where_parameters: Dict[str, Any] = {
            "field": self,
            "operator": operators.InOperator,
        }

        if subquery:
            where_parameters["comparison_value"] = subquery
        elif comparison_values:
            where_parameters["comparison_values"] = comparison_values

        return Filter(**where_parameters)

    def not_in(
        self: Self,
        *comparison_values: FieldType,
        subquery: Optional[SQLSelectable] = None,
    ) -> Filter:
        if subquery and comparison_values:
            raise FilterComparisonError(
                "It's not possible to specify subquery "
                "with positional arguments in `not_in` method. "
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

        where_parameters: Dict[str, Any] = {
            "field": self,
            "operator": operators.NotInOperator,
        }

        if subquery:
            where_parameters["comparison_value"] = subquery
        elif comparison_values:
            where_parameters["comparison_values"] = comparison_values

        return Filter(**where_parameters)

    def between(
        self: Self,
        left_value: Union[FieldType, "Field[Any]"],
        right_value: Union[FieldType, "Field[Any]"],
    ) -> FilterBetween:
        is_valid_type: Final[bool] = all(
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
        self: Self,
    ) -> str:
        return (
            f"{self._original_field_name} {self._sql_type} "
            f"{self._field_null} {self._field_default}"
        )

    def querystring(self: Self) -> QueryString:
        return QueryString(
            self.field_name,
            sql_template="{}",
        )

    def __eq__(  # type: ignore[override]
        self: Self,
        comparison_value: Union[FieldType, "Field[Any]", OperatorTypes],
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
        self: Self,
        comparison_value: Union[FieldType, "Field[Any]", OperatorTypes],
    ) -> Filter:
        return self.__eq__(comparison_value)

    def __ne__(  # type: ignore[override]
        self: Self,
        comparison_value: Union[FieldType, "Field[Any]", OperatorTypes],
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
        self: Self,
        comparison_value: Union[FieldType, OperatorTypes],
    ) -> Filter:
        return self.__ne__(comparison_value)

    def __gt__(
        self: Self,
        comparison_value: Union[FieldType, OperatorTypes],
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
        self: Self,
        comparison_value: Union[FieldType, OperatorTypes],
    ) -> Filter:
        return self.__gt__(comparison_value)

    def __ge__(
        self: Self,
        comparison_value: Union[FieldType, OperatorTypes],
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
        self: Self,
        comparison_value: Union[FieldType, OperatorTypes],
    ) -> Filter:
        return self.__ge__(comparison_value)

    def __lt__(
        self: Self,
        comparison_value: Union[FieldType, OperatorTypes],
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
        self: Self,
        comparison_value: Union[FieldType, OperatorTypes],
    ) -> Filter:
        return self.__lt__(comparison_value)

    def __le__(
        self: Self,
        comparison_value: Union[FieldType, OperatorTypes],
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
        self: Self,
        comparison_value: Union[FieldType, OperatorTypes],
    ) -> Filter:
        return self.__le__(comparison_value)

    def _is_the_same_field(
        self: Self,
        second_field: "BaseField[FieldType]",
    ) -> bool:
        """Compare two fields.

        Return `True` if they are the same, else `False`.
        They are equal if they `_field_data`s are the same.
        """
        return self._field_data == second_field._field_data

    def _with_prefix(self: Self, prefix: str) -> "Field[FieldType]":
        """Give Field a prefix.

        Make a Field deepcopy and set new prefix.

        ### Parameters
        - `prefix`: prefix for the field.

        ### Returns
        `Field` with new prefix.
        """
        field: Field[FieldType] = copy.deepcopy(self)
        field._field_data.prefix = prefix
        return field

    def _with_alias(self: Self, alias: str) -> "Field[FieldType]":
        """Give Field an alias.

        Make a Field deepcopy and set new alias.

        ### Parameters
        - `alias`: alias for the field.

        ### Returns
        `Field` with new alias.
        """
        field: Field[FieldType] = copy.deepcopy(self)
        field._field_data.alias = alias
        return field

    def _validate_field_value(
        self: Self,
        field_value: Optional[FieldType],
    ) -> None:
        """Validate field value.

        If `field_value` is None but field declared as NOT NULL,
        throw an error.

        :param field_value: new value for the field.

        :raises FieldValueValidationError: if the `max_length` is exceeded.
        """
        if field_value is None and self._is_null:
            return

    def _validate_default_value(
        self: Self,
        default_value: FieldDefaultType[FieldType],
    ) -> None:
        if default_value is None or types.FunctionType == type(default_value):
            return

        try:
            self._validate_field_value(
                field_value=default_value,  # type: ignore[arg-type]
            )
        except FieldValueValidationError as exc:
            raise FieldValueValidationError(
                f"Wrong default value in the field "
                f"{self.__class__.__name__}",
            ) from exc

        if not isinstance(default_value, self._set_available_types):
            raise FieldValueValidationError(
                f"Wrong default value in the field "
                f"{self.__class__.__name__}",
            )
