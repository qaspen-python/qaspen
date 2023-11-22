from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any, Final, Union

from qaspen.base.operators import AllOperator, AnyOperator
from qaspen.exceptions import (
    FieldComparisonError,
    FieldDeclarationError,
    FieldValueValidationError,
)
from qaspen.fields import operators
from qaspen.fields.base import Field
from qaspen.qaspen_types import FieldDefaultType, FieldType
from qaspen.sql_type import primitive_types
from qaspen.statements.combinable_statements.filter_statement import Filter

if TYPE_CHECKING:  # pragma: no cover
    from typing_extensions import Self


class BaseIntegerField(Field[int]):
    """Base field for all integer fields."""

    _available_comparison_types: tuple[type, ...] = (
        int,
        float,
        Field,
        AnyOperator,
        AllOperator,
    )
    _set_available_types: tuple[type, ...] = (int, float)
    _available_max_value: int | None
    _available_min_value: int | None

    def __init__(
        self: Self,
        *pos_arguments: Any,
        is_null: bool = False,
        default: int | None = None,
        db_field_name: str | None = None,
        maximum: float | None = None,
        minimum: float | None = None,
    ) -> None:
        self._maximum: Final = maximum
        self._minimum: Final = minimum

        super().__init__(
            *pos_arguments,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )

    def _validate_field_value(
        self: Self,
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

        is_max_value_reached: Final = bool(
            field_value
            and self._available_max_value
            and field_value > self._available_max_value,
        )
        if is_max_value_reached:
            max_reached_err_msg: Final = (
                f"Field value - `{field_value}` "
                f"is greater than field `{self.__class__.__name__}` "
                f"can accommodate - `{self._available_max_value}`",
            )
            raise FieldValueValidationError(max_reached_err_msg)

        is_min_value_reached: Final = bool(
            field_value
            and self._available_min_value
            and field_value < self._available_min_value,
        )
        if is_min_value_reached:
            min_reached_err_msg: Final = (
                f"Field value - `{field_value}` "
                f"is less than field `{self.__class__.__name__}` "
                f"can accommodate - `{self._available_min_value}`",
            )
            raise FieldValueValidationError(min_reached_err_msg)
        if field_value and self._maximum and field_value > self._maximum:
            value_max_reached_err_msg: Final = (
                f"Field value - `{field_value}` is greater "
                f"than maximum you set `{self._maximum}`",
            )
            raise FieldValueValidationError(value_max_reached_err_msg)
        if field_value and self._minimum and field_value < self._minimum:
            value_min_reached_err_msg: Final = (
                f"Field value - `{field_value}` is less "
                f"than minimum you set `{self._minimum}`",
            )
            raise FieldValueValidationError(value_min_reached_err_msg)


class SmallIntField(BaseIntegerField):
    """SMALLINT field."""

    _available_max_value: int = 32767
    _available_min_value: int = -32768
    _sql_type = primitive_types.SmallInt


class IntegerField(BaseIntegerField):
    """INTEGER field."""

    _available_max_value: int = 2147483647
    _available_min_value: int = -2147483648
    _sql_type = primitive_types.Integer


class BigIntField(BaseIntegerField):
    """BIGINT field."""

    _available_max_value: int = 9223372036854775807
    _available_min_value: int = -9223372036854775808
    _sql_type = primitive_types.BigInt


class NumericField(BaseIntegerField):
    """NUMERIC field."""

    _sql_type = primitive_types.Numeric
    _available_max_value = None
    _available_min_value = None

    def __init__(
        self: Self,
        *pos_arguments: Any,
        precision: int | None = None,
        scale: int | None = None,
        is_null: bool = False,
        default: int | None = None,
        db_field_name: str | None = None,
        maximum: float | None = None,
        minimum: float | None = None,
    ) -> None:
        if not precision and scale:
            declaration_err_msg: Final = (
                "You cannot specify `scale` without `precision`.",
            )
            raise FieldDeclarationError(declaration_err_msg)

        super().__init__(
            *pos_arguments,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
            maximum=maximum,
            minimum=minimum,
        )

        self.precision: int | None = precision
        self.scale: int | None = scale

    @property
    def _field_type(self: Self) -> str:
        field_type: str = self._sql_type.sql_type()
        if self.precision and self.scale:
            field_type += f"({self.precision}, {self.scale})"
        elif self.precision:
            field_type += f"({self.precision})"

        return field_type


class DecimalField(NumericField):
    """DECIMAL field.

    The same as `Numeric` field.
    """

    _sql_type = primitive_types.Decimal  # type: ignore[assignment]


class RealField(Field[Union[float, int, str]]):
    """REAL field."""

    _available_comparison_types: tuple[type, ...] = (
        str,
        int,
        float,
        Field,
        AnyOperator,
        AllOperator,
    )
    _set_available_types: tuple[type, ...] = (str, float, int)
    _sql_type = primitive_types.Real

    def __init__(
        self: Self,
        *pos_arguments: Any,
        is_null: bool = False,
        default: int | str | None = None,
        db_field_name: str | None = None,
    ) -> None:
        super().__init__(  # pragma: no cover
            *pos_arguments,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )


class DoublePrecisionField(Field[Union[int, float, str]]):
    """DOUBLE PRECISION field."""

    _available_comparison_types: tuple[type, ...] = (
        str,
        int,
        float,
        Field,
        AnyOperator,
        AllOperator,
    )
    _set_available_types: tuple[type, ...] = (str, int, float)
    _sql_type = primitive_types.DoublePrecision

    def __init__(
        self: Self,
        *pos_arguments: Any,
        is_null: bool = False,
        default: int | str | None = None,
        db_field_name: str | None = None,
    ) -> None:
        super().__init__(  # pragma: no cover
            *pos_arguments,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )


class BooleanField(Field[bool]):
    """BOOLEAN field."""

    _available_comparison_types: tuple[type, ...] = (
        bool,
        Field,
        AnyOperator,
        AllOperator,
    )
    _set_available_types: tuple[type, ...] = (bool,)
    _sql_type = primitive_types.Boolean


class SerialBaseField(BaseIntegerField):
    """Base Serial field for all possible SERIAL fields."""

    def __init__(
        self: Self,
        *pos_arguments: Any,
        db_field_name: str | None = None,
        maximum: int | None = None,
        minimum: int | None = None,
        next_val_seq_name: str | None = None,
    ) -> None:
        """Create field with Serial signature.

        You can create name for the `next_val` sequence.
        Pass a name in the `next_val_seq_name` parameter
        in field creation.

        :param db_field_name: name for the field in the database.
        :param maximum: max number for the field at python level.
        :param minimum: min number for the field at python level.
        :param next_val_seq_name: name for the `nextval` sequence.
        """
        super().__init__(
            *pos_arguments,
            is_null=False,
            default=None,
            db_field_name=db_field_name,
            maximum=maximum,
            minimum=minimum,
        )

        self.next_val_seq_name: str | None = next_val_seq_name


class SmallSerialField(SerialBaseField, SmallIntField):
    """SMALLSERIAL field.

    Its `SmallInt` field with `NOT NULL` property,
    and autoincrement functionality.
    """


class SerialField(SerialBaseField, IntegerField):
    """SERIAL field.

    Its `Integer` field with `NOT NULL` property,
    and autoincrement functionality.
    """

    _sub_field: str = "INTEGER"


class BigSerialField(SerialBaseField, BigIntField):
    """BIGSERIAL field.

    Its `BigInt` field with `NOT NULL` property,
    and autoincrement functionality.
    """

    _sub_field: str = "BIGINT"


AvailableComparisonTypes = (
    str,
    Field,
    AnyOperator,
    AllOperator,
)


class BaseStringField(Field[str]):
    """Base Field for all string-related Fields.

    It adds LIKE, NOT LIKE, ILIKE and NOT ILIKE methods.
    """

    _available_comparison_types: tuple[type, ...] = AvailableComparisonTypes
    _set_available_types: tuple[type, ...] = (str,)

    def like(
        self: Self,
        comparison_value: str,
    ) -> Filter:
        """`LIKE` PostgreSQL clause.

        It allows you to use `LIKE` clause.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.LikeOperator,
            )

        comparison_err_msg: Final = (
            f"It's impossible to use `LIKE` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )
        raise FieldComparisonError(comparison_err_msg)

    def not_like(
        self: Self,
        comparison_value: str,
    ) -> Filter:
        """`NOT LIKE` PostgreSQL clause.

        It allows you to use `NOT LIKE` clause.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.NotLikeOperator,
            )

        comparison_err_msg: Final = (
            f"It's impossible to use `NOT LIKE` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )
        raise FieldComparisonError(comparison_err_msg)

    def ilike(
        self: Self,
        comparison_value: str,
    ) -> Filter:
        """`ILIKE` PostgreSQL clause.

        It allows you to use `ILIKE` clause.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.ILikeOperator,
            )
        comparison_err_msg: Final = (
            f"It's impossible to use `ILIKE` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )
        raise FieldComparisonError(comparison_err_msg)

    def not_ilike(
        self: Self,
        comparison_value: str,
    ) -> Filter:
        """`NOT ILIKE` PostgreSQL clause.

        It allows you to use `NOT ILIKE` clause.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.NotILikeOperator,
            )

        comparison_err_msg: Final = (
            f"It's impossible to use `NOT ILIKE` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )
        raise FieldComparisonError(comparison_err_msg)


class VarCharField(BaseStringField):
    """Varchar Field.

    Behave like normal PostgreSQL VARCHAR field.
    """

    _sql_type = primitive_types.VarChar

    def __init__(
        self: Self,
        *args: Any,
        max_length: int = 255,
        is_null: bool = False,
        default: str | None = None,
        db_field_name: str | None = None,
    ) -> None:
        self._max_length: int = max_length

        super().__init__(
            *args,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )

    def _validate_field_value(
        self: Self,
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
        if field_value and len(field_value) > self._max_length:
            validation_err_msg: Final = (
                f"You cannot set value with length {len(field_value)} "
                f"to the {self.__class__.__name__} with "
                f"`max_length` - {self._max_length}",
            )
            raise FieldValueValidationError(validation_err_msg)

    @property
    def _field_type(self: Self) -> str:
        return f"{self._sql_type.sql_type()}({self._max_length})"


class TextField(BaseStringField):
    """Text field.

    Behave like normal PostgreSQL TEXT field.
    """

    _sql_type = primitive_types.Text

    def __init__(
        self: Self,
        *args: Any,
        is_null: bool = False,
        default: str | None = None,
        db_field_name: str | None = None,
    ) -> None:
        super().__init__(
            *args,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )


class CharField(Field[str]):
    """Char field.

    You cannot specify `max_length` parameter for this Field,
    it's always 1.

    If you want more characters, use `VarChar` field.
    """

    _available_comparison_types: tuple[type, ...] = AvailableComparisonTypes
    _set_available_types: tuple[type, ...] = (str,)
    _sql_type = primitive_types.Char

    def __init__(
        self: Self,
        *pos_arguments: Any,
        is_null: bool = False,
        default: str | None = None,
        db_field_name: str | None = None,
    ) -> None:
        super().__init__(  # pragma: no cover
            *pos_arguments,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )

    def _validate_field_value(
        self: Self,
        field_value: str | None,
    ) -> None:
        """Validate field value.

        If value length not equal 1 raise an error.

        :param field_value: new value for the field.

        :raises FieldValueValidationError: if value length not equal 1.
        """
        super()._validate_field_value(
            field_value=field_value,
        )

        if not field_value or len(field_value) == 1:
            return

        validation_err_msg: Final = (
            f"CHAR field must always contain "
            f"only one character. "
            f"You tried to set {field_value}",
        )
        raise FieldValueValidationError(validation_err_msg)


class BaseDatetimeField(Field[FieldType]):
    """Base Field for all Date/Time fields."""

    _database_default: str = "CURRENT_DATE"

    def __init__(
        self: Self,
        *args: Any,
        is_null: bool = False,
        db_field_name: str | None = None,
        default: FieldDefaultType[FieldType] = None,
        database_default: bool = False,
    ) -> None:
        if default and database_default:
            declaration_err_msg: Final = (
                "Please specify either `default` or `database_default` "
                "for DateField.",
            )
            raise FieldDeclarationError(declaration_err_msg)

        super().__init__(
            *args,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )

        self.database_default: Final = database_default

    @property
    def _field_default(self: Self) -> str:
        """Build DEFAULT string for a field.

        :returns: DEFAULT sql string for database.
        """
        if not self.database_default:
            return super()._field_default
        return f"DEFAULT {self._database_default}"


class BaseDateTimeFieldWithTZ(BaseDatetimeField[FieldType]):
    """Base Field for all Date/Time fields with TimeZone."""

    def __init__(
        self: Self,
        *args: Any,
        is_null: bool = False,
        db_field_name: str | None = None,
        default: FieldDefaultType[FieldType] = None,
        database_default: bool = False,
        with_timezone: bool = False,
    ) -> None:
        if default and database_default:
            declaration_err_msg: Final = (
                "Please specify either `default` or `database_default` "
                "for DateField.",
            )
            raise FieldDeclarationError(declaration_err_msg)

        super().__init__(
            *args,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
            database_default=database_default,
        )

        self.with_timezone: Final = with_timezone

    @property
    def _field_type(self: Self) -> str:
        if self.with_timezone:
            return f"{self._sql_type.sql_type()} WITH TIME ZONE"
        return super()._field_type


class DateField(BaseDatetimeField[datetime.date]):
    """PostgreSQL type for `datetime.date` python type."""

    _available_comparison_types: tuple[
        type,
        ...,
    ] = (
        datetime.date,
        Field,
        AllOperator,
        AnyOperator,
    )
    _set_available_types: tuple[type, ...] = (datetime.date,)
    _sql_type = primitive_types.Date


class TimeField(BaseDateTimeFieldWithTZ[datetime.time]):
    """PostgreSQL type for `datetime.time` python type."""

    _database_default: str = "CURRENT_TIME"

    _available_comparison_types: tuple[
        type,
        ...,
    ] = (
        datetime.time,
        Field,
        AllOperator,
        AnyOperator,
    )
    _set_available_types: tuple[type, ...] = (datetime.time,)
    _sql_type = primitive_types.Time


class TimestampField(BaseDateTimeFieldWithTZ[datetime.datetime]):
    """PostgreSQL type for `datetime.datetime` python type."""

    _available_comparison_types: tuple[
        type,
        ...,
    ] = (
        datetime.datetime,
        Field,
        AllOperator,
        AnyOperator,
    )
    _set_available_types: tuple[type, ...] = (datetime.datetime,)
    _sql_type = primitive_types.Timestamp


class IntervalField(Field[datetime.timedelta]):
    """PostgreSQL type for `datetime.timedelta` python type."""

    _available_comparison_types: tuple[
        type,
        ...,
    ] = (
        datetime.timedelta,
        Field,
        AllOperator,
        AnyOperator,
    )
    _set_available_types: tuple[type, ...] = (datetime.timedelta,)
    _sql_type = primitive_types.Interval
