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
from qaspen.fields.base_field import BaseField, FieldData, FieldType
from qaspen.fields.utils import validate_max_length
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.filter_statement import (
    Filter,
    FilterBetween,
)

if typing.TYPE_CHECKING:
    from qaspen.table.base_table import BaseTable

OperatorTypes = AnyOperator | AllOperator


class Field(BaseField[FieldType], SQLSelectable):
    _available_comparison_types: tuple[type, ...]
    _set_available_types: tuple[type, ...]

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

        if db_field_name:
            self._field_name: str = db_field_name
        else:
            self._field_name = ""

        self._default: FieldType | None = self._validate_default_value(
            default_value=default,
        )

        self._field_data: FieldData[FieldType] = FieldData(
            field_name=db_field_name if db_field_name else self._field_name,
            field_value=None,
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
                instance.__dict__[self.field_name_clear],
            )
        except (AttributeError, KeyError):
            return typing.cast(
                Field[FieldType],
                owner.__dict__[self.field_name_clear],
            )

    def __set__(self: typing.Self, instance: object, value: FieldType) -> None:
        if isinstance(value, self.__class__):
            instance.__dict__[self.field_name_clear] = value
            return
        if not isinstance(value, self._set_available_types):
            raise TypeError(
                f"Can't assign not string type to {self.__class__.__name__}",
            )
        self._validate_field_value(
            field_value=value,
        )
        field: Field[FieldType] = instance.__dict__[self.field_name_clear]
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
        if field_value is None and self._is_null:
            return
        if field_value is None and not self._is_null:
            raise FieldValueValidationError(
                f"You can't assign `None` to the field "
                f"that declared as `NOT NULL`",
            )

    def _validate_default_value(
        self: typing.Self,
        default_value: FieldType | None,
    ) -> None:
        if default_value is None:
            return
        try:
            self._validate_field_value(
                field_value=default_value,
            )
        except FieldValueValidationError as exc:
            raise FieldValueValidationError(
                f"Wrong default value in the field {self._field_name}",
            ) from exc


class BaseStringField(Field[str]):
    _available_comparison_types: tuple[type, ...] = (
        str,
        Field,
        AnyOperator,
        AllOperator,
    )
    _set_available_types: tuple[type, ...] = (str,)

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
        if max_length:
            validate_max_length(max_length=max_length)

        self._max_length: int = max_length if max_length else 100

        super().__init__(
            *pos_arguments,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )

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

    def _validate_field_value(
        self: typing.Self,
        field_value: str | None,
    ) -> None:
        """Validate field value.

        If new value has length more that `max_length`, throw an error.

        :param field_value: new value for the field.

        :raises FieldValueValidationError: if the `max_length` is exceeded.
        """
        super()._validate_field_value(
            field_value=field_value,
        )

        if field_value and len(field_value) <= self._max_length:
            return
        elif field_value and len(field_value) > self._max_length:
            raise FieldValueValidationError(
                f"You cannot set value with length {len(field_value)} "
                f"to the {self.__class__.__name__} with "
                f"`max_length` - {self._max_length}",
            )

    def _build_fields_sql_type(self: typing.Self) -> str:
        return f"{self._default_field_type}({self._max_length})"


class VarChar(BaseStringField):
    """Varchar Field.

    Behave like normal PostgreSQL VARCHAR field.
    """


class Text(Field[str]):
    """Text field.

    Behave like normal PostgreSQL TEXT field.
    """

    _set_available_types: tuple[type, ...] = (str,)


class Char(Field[str]):
    """Char field.

    You cannot specify `max_length` parameter for this Field,
    it's always 1.

    If you want more characters, use `VarChar` field.
    """

    _set_available_types: tuple[type, ...] = (str,)

    def __init__(
        self: typing.Self,
        *pos_arguments: typing.Any,
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

    def _validate_field_value(
        self: typing.Self,
        field_value: str | None,
    ) -> None:
        """Validate field value.

        If value length not equal 1 raise an error.

        :param field_value: new value for the field.

        :raises FieldValueValidationError: if value length not equal 1.
        """
        if not field_value:
            return
        if len(field_value) == 1:
            return

        raise FieldValueValidationError(
            f"CHAR field must always contain "
            f"only one character. "
            f"You tried to set {field_value}",
        )


class BaseIntegerField(Field[int]):
    """Base field for all integer fields."""

    _available_comparison_types: tuple[type, ...] = (
        int,
        Field,
        AnyOperator,
        AllOperator,
    )
    _set_available_types: tuple[type, ...] = (int,)
    _available_max_value: int
    _available_min_value: int

    def __init__(
        self: typing.Self,
        *pos_arguments: typing.Any,
        is_null: bool = False,
        default: int | None = None,
        db_field_name: str | None = None,
        maximum: int | None = None,
        minimum: int | None = None,
    ) -> None:
        # TODO: Added CHECK constraint to these params
        self._maximum: int | None = maximum
        self._minimum: int | None = minimum

        super().__init__(
            *pos_arguments,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )

    def _validate_field_value(
        self: typing.Self,
        field_value: int | None,
    ) -> None:
        """Validate field value.

        Check maximum and minimum values, if validation failed
        raise an error.

        :param field_value: new value for the field.

        :raises FieldValueValidationError: if value is too big or small.
        """
        super()._validate_field_value(
            field_value=field_value,
        )
        if field_value and field_value > self._available_max_value:
            raise FieldValueValidationError(
                f"Field value - {field_value} "
                f"is bigger than field {self.__class__.__name__} "
                f"can accommodate - {self._available_max_value}",
            )
        if field_value and field_value < self._available_min_value:
            raise FieldValueValidationError(
                f"Field value - {field_value} "
                f"is less than field {self.__class__.__name__} "
                f"can accommodate - {self._available_min_value}",
            )
        if field_value and self._maximum and field_value > self._maximum:
            raise FieldValueValidationError(
                f"Field value - {field_value} "
                f"is bigger than maximum you set {self._maximum}",
            )
        if field_value and self._minimum and field_value < self._minimum:
            raise FieldValueValidationError(
                f"Field value - {field_value} "
                f"is less than minimum you set {self._minimum}",
            )


class SmallInt(BaseIntegerField):
    """Integer SmallInt field."""

    _available_max_value: int = 32767
    _available_min_value: int = -32768
