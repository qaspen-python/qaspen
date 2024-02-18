from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any, Callable, Final, Union

from qaspen.base.comparison_operators import (
    ILikeComparisonMixin,
    LikeComparisonMixin,
    NotILikeComparisonMixin,
    NotLikeComparisonMixin,
)
from qaspen.base.operators import All_, Any_
from qaspen.columns.base import Column
from qaspen.exceptions import (
    ColumnDeclarationError,
    ColumnValueValidationError,
)
from qaspen.qaspen_types import ColumnDefaultType, ColumnType
from qaspen.sql_type import primitive_types

if TYPE_CHECKING:
    from typing_extensions import Self


class BaseIntegerColumn(Column[Union[int, float]]):
    """Base column for all integer columns."""

    _available_comparison_types: tuple[type, ...] = (
        int,
        float,
        Column,
        Any_,
        All_,
    )
    _set_available_types: tuple[type, ...] = (int, float)
    _available_max_value: int | None
    _available_min_value: int | None

    def __init__(
        self: Self,
        *pos_arguments: Any,
        is_null: bool = True,
        default: int | Callable[[], int] | None = None,
        database_default: str | None = None,
        db_column_name: str | None = None,
        maximum: float | None = None,
        minimum: float | None = None,
    ) -> None:
        self._maximum: Final = maximum
        self._minimum: Final = minimum

        super().__init__(
            *pos_arguments,
            is_null=is_null,
            default=default,
            db_column_name=db_column_name,
            database_default=database_default,
        )

    def _validate_column_value(
        self: Self,
        column_value: float | None,
    ) -> None:
        """Validate column value.

        Check maximum and minimum values, if validation failed
        raise an error.

        :param column_value: new value for the column.

        :raises ColumnValueValidationError: if value is too big or small.
        """
        super()._validate_column_value(
            column_value=column_value,
        )

        is_max_value_reached: Final = bool(
            column_value
            and self._available_max_value
            and column_value > self._available_max_value,
        )
        if is_max_value_reached:
            max_reached_err_msg: Final = (
                f"Column value - `{column_value}` "
                f"is greater than column `{self.__class__.__name__}` "
                f"can accommodate - `{self._available_max_value}`",
            )
            raise ColumnValueValidationError(max_reached_err_msg)

        is_min_value_reached: Final = bool(
            column_value
            and self._available_min_value
            and column_value < self._available_min_value,
        )
        if is_min_value_reached:
            min_reached_err_msg: Final = (
                f"Column value - `{column_value}` "
                f"is less than column `{self.__class__.__name__}` "
                f"can accommodate - `{self._available_min_value}`",
            )
            raise ColumnValueValidationError(min_reached_err_msg)
        if column_value and self._maximum and column_value > self._maximum:
            value_max_reached_err_msg: Final = (
                f"Column value - `{column_value}` is greater "
                f"than maximum you set `{self._maximum}`",
            )
            raise ColumnValueValidationError(value_max_reached_err_msg)
        if column_value and self._minimum and column_value < self._minimum:
            value_min_reached_err_msg: Final = (
                f"Column value - `{column_value}` is less "
                f"than minimum you set `{self._minimum}`",
            )
            raise ColumnValueValidationError(value_min_reached_err_msg)


class SmallIntColumn(BaseIntegerColumn):
    """SMALLINT column."""

    _available_max_value: int = 32767
    _available_min_value: int = -32768
    _sql_type = primitive_types.SmallInt


class IntegerColumn(BaseIntegerColumn):
    """INTEGER column."""

    _available_max_value: int = 2147483647
    _available_min_value: int = -2147483648
    _sql_type = primitive_types.Integer


class BigIntColumn(BaseIntegerColumn):
    """BIGINT column."""

    _available_max_value: int = 9223372036854775807
    _available_min_value: int = -9223372036854775808
    _sql_type = primitive_types.BigInt


class NumericColumn(BaseIntegerColumn):
    """NUMERIC column."""

    _sql_type = primitive_types.Numeric
    _available_max_value = None
    _available_min_value = None

    def __init__(
        self: Self,
        *pos_arguments: Any,
        precision: int | None = None,
        scale: int | None = None,
        is_null: bool = True,
        default: int | Callable[[], int] | None = None,
        database_default: str | None = None,
        db_column_name: str | None = None,
        maximum: float | None = None,
        minimum: float | None = None,
    ) -> None:
        if not precision and scale:
            declaration_err_msg: Final = (
                "You cannot specify `scale` without `precision`.",
            )
            raise ColumnDeclarationError(declaration_err_msg)

        super().__init__(
            *pos_arguments,
            is_null=is_null,
            default=default,
            db_column_name=db_column_name,
            database_default=database_default,
            maximum=maximum,
            minimum=minimum,
        )

        self.precision: int | None = precision
        self.scale: int | None = scale

    @property
    def _column_type(self: Self) -> str:
        column_type: str = self._sql_type.sql_type()
        if self.precision and self.scale:
            column_type += f"({self.precision}, {self.scale})"
        elif self.precision:
            column_type += f"({self.precision})"

        return column_type


class DecimalColumn(NumericColumn):
    """DECIMAL column.

    The same as `Numeric` column.
    """

    _sql_type = primitive_types.Decimal  # type: ignore[assignment]


class RealColumn(Column[Union[str, int, float]]):
    """REAL column."""

    _available_comparison_types: tuple[type, ...] = (
        str,
        int,
        float,
        Column,
        Any_,
        All_,
    )
    _set_available_types: tuple[type, ...] = (str, int, float)
    _sql_type = primitive_types.Real

    def __init__(
        self: Self,
        *pos_arguments: Any,
        is_null: bool = True,
        default: int | str | Callable[[], str | int] | None = None,
        database_default: str | None = None,
        db_column_name: str | None = None,
    ) -> None:
        super().__init__(  # pragma: no cover
            *pos_arguments,
            is_null=is_null,
            default=default,
            database_default=database_default,
            db_column_name=db_column_name,
        )


class DoublePrecisionColumn(Column[Union[int, float, str]]):
    """DOUBLE PRECISION column."""

    _available_comparison_types: tuple[type, ...] = (
        str,
        int,
        float,
        Column,
        Any_,
        All_,
    )
    _set_available_types: tuple[type, ...] = (str, int, float)
    _sql_type = primitive_types.DoublePrecision

    def __init__(
        self: Self,
        *pos_arguments: Any,
        is_null: bool = True,
        default: int | str | Callable[[], str | int] | None = None,
        database_default: str | None = None,
        db_column_name: str | None = None,
    ) -> None:
        super().__init__(  # pragma: no cover
            *pos_arguments,
            is_null=is_null,
            default=default,
            database_default=database_default,
            db_column_name=db_column_name,
        )


class BooleanColumn(Column[bool]):
    """BOOLEAN column."""

    _available_comparison_types: tuple[type, ...] = (
        bool,
        Column,
        Any_,
        All_,
    )
    _set_available_types: tuple[type, ...] = (bool,)
    _sql_type = primitive_types.Boolean


class SerialBaseColumn(BaseIntegerColumn):
    """Base Serial column for all possible SERIAL columns."""

    def __init__(
        self: Self,
        *pos_arguments: Any,
        db_column_name: str | None = None,
        maximum: int | None = None,
        minimum: int | None = None,
        next_val_seq_name: str | None = None,
    ) -> None:
        """Create column with Serial signature.

        You can create name for the `next_val` sequence.
        Pass a name in the `next_val_seq_name` parameter
        in column creation.

        :param db_column_name: name for the column in the database.
        :param maximum: max number for the column at python level.
        :param minimum: min number for the column at python level.
        :param next_val_seq_name: name for the `nextval` sequence.
        """
        self.python_is_null = True

        super().__init__(
            *pos_arguments,
            is_null=False,
            default=None,
            db_column_name=db_column_name,
            maximum=maximum,
            minimum=minimum,
        )

        self.next_val_seq_name: str | None = next_val_seq_name


class SmallSerialColumn(SerialBaseColumn, SmallIntColumn):
    """SMALLSERIAL column.

    Its `SmallInt` column with `NOT NULL` property,
    and autoincrement functionality.
    """


class SerialColumn(SerialBaseColumn, IntegerColumn):
    """SERIAL column.

    Its `Integer` column with `NOT NULL` property,
    and autoincrement functionality.
    """

    _sub_column: str = "INTEGER"


class BigSerialColumn(SerialBaseColumn, BigIntColumn):
    """BIGSERIAL column.

    Its `BigInt` column with `NOT NULL` property,
    and autoincrement functionality.
    """

    _sub_column: str = "BIGINT"


AvailableComparisonTypes = (
    str,
    Column,
    Any_,
    All_,
)


class BaseStringColumn(
    Column[str],
    LikeComparisonMixin[str],
    NotLikeComparisonMixin[str],
    ILikeComparisonMixin[str],
    NotILikeComparisonMixin[str],
):
    """Base Column for all string-related Columns.

    It adds LIKE, NOT LIKE, ILIKE and NOT ILIKE methods.
    """

    _available_comparison_types: tuple[type, ...] = AvailableComparisonTypes
    _set_available_types: tuple[type, ...] = (str,)


class VarCharColumn(BaseStringColumn):
    """Varchar Column.

    Behave like normal PostgreSQL VARCHAR column.
    """

    _sql_type = primitive_types.VarChar

    def __init__(
        self: Self,
        *args: Any,
        max_length: int = 255,
        is_null: bool = True,
        default: str | Callable[[], str] | None = None,
        database_default: str | None = None,
        db_column_name: str | None = None,
    ) -> None:
        self._max_length: int = max_length

        super().__init__(
            *args,
            is_null=is_null,
            default=default,
            database_default=database_default,
            db_column_name=db_column_name,
        )

    def _validate_column_value(
        self: Self,
        column_value: str | None,
    ) -> None:
        """Validate column value.

        If new value has length more that `max_length`, throw an error.

        :param column_value: new value for the column.

        :raises ColumnValueValidationError: if the `max_length` is exceeded.
        """
        super()._validate_column_value(
            column_value=column_value,
        )

        if column_value and len(column_value) <= self._max_length:
            return
        if column_value and len(column_value) > self._max_length:
            validation_err_msg: Final = (
                f"You cannot set value with length {len(column_value)} "
                f"to the {self.__class__.__name__} with "
                f"`max_length` - {self._max_length}",
            )
            raise ColumnValueValidationError(validation_err_msg)

    @property
    def _column_type(self: Self) -> str:
        return f"{self._sql_type.sql_type()}({self._max_length})"


class TextColumn(BaseStringColumn):
    """Text column.

    Behave like normal PostgreSQL TEXT column.
    """

    _sql_type = primitive_types.Text

    def __init__(
        self: Self,
        *args: Any,
        is_null: bool = True,
        default: str | Callable[[], str] | None = None,
        database_default: str | None = None,
        db_column_name: str | None = None,
    ) -> None:
        super().__init__(
            *args,
            is_null=is_null,
            default=default,
            database_default=database_default,
            db_column_name=db_column_name,
        )


class CharColumn(Column[str]):
    """Char column.

    You cannot specify `max_length` parameter for this Column,
    it's always 1.

    If you want more characters, use `VarChar` column.
    """

    _available_comparison_types: tuple[type, ...] = AvailableComparisonTypes
    _set_available_types: tuple[type, ...] = (str,)
    _sql_type = primitive_types.Char

    def __init__(
        self: Self,
        *pos_arguments: Any,
        is_null: bool = True,
        default: str | Callable[[], str] | None = None,
        database_default: str | None = None,
        db_column_name: str | None = None,
    ) -> None:
        super().__init__(  # pragma: no cover
            *pos_arguments,
            is_null=is_null,
            default=default,
            database_default=database_default,
            db_column_name=db_column_name,
        )

    def _validate_column_value(
        self: Self,
        column_value: str | None,
    ) -> None:
        """Validate column value.

        If value length not equal 1 raise an error.

        :param column_value: new value for the column.

        :raises ColumnValueValidationError: if value length not equal 1.
        """
        super()._validate_column_value(
            column_value=column_value,
        )

        if not column_value or len(column_value) == 1:
            return

        validation_err_msg: Final = (
            f"CHAR column must always contain "
            f"only one character. "
            f"You tried to set {column_value}",
        )
        raise ColumnValueValidationError(validation_err_msg)


class BaseDatetimeColumn(Column[ColumnType]):
    """Base Column for all Date/Time columns."""

    def __init__(
        self: Self,
        *args: Any,
        is_null: bool = True,
        db_column_name: str | None = None,
        default: ColumnDefaultType[ColumnType] = None,
        database_default: str | None = None,
    ) -> None:
        if default and database_default:
            declaration_err_msg: Final = (
                "Please specify either `default` or `database_default` "
                "for DateColumn.",
            )
            raise ColumnDeclarationError(declaration_err_msg)

        super().__init__(
            *args,
            is_null=is_null,
            default=default,
            db_column_name=db_column_name,
            database_default=database_default,
        )

    @property
    def _column_default(self: Self) -> str:
        """Build DEFAULT string for a column.

        :returns: DEFAULT sql string for database.
        """
        if not self.database_default:
            return super()._column_default
        return f"DEFAULT {self.database_default}"


class BaseDateTimeColumnWithTZ(BaseDatetimeColumn[ColumnType]):
    """Base Column for all Date/Time columns with TimeZone."""

    def __init__(
        self: Self,
        *args: Any,
        is_null: bool = True,
        db_column_name: str | None = None,
        default: ColumnDefaultType[ColumnType] = None,
        database_default: str | None = None,
    ) -> None:
        super().__init__(
            *args,
            is_null=is_null,
            default=default,
            db_column_name=db_column_name,
            database_default=database_default,
        )

        self.with_timezone: Final = True


class DateColumn(BaseDatetimeColumn[datetime.date]):
    """PostgreSQL type for `datetime.date` python type."""

    _available_comparison_types: tuple[
        type,
        ...,
    ] = (
        datetime.date,
        Column,
        All_,
        Any_,
    )
    _set_available_types: tuple[type, ...] = (datetime.date,)
    _sql_type = primitive_types.Date

    def __init__(
        self: Self,
        *args: Any,
        is_null: bool = True,
        db_column_name: str | None = None,
        default: datetime.date | Callable[[], datetime.date] | None = None,
        database_default: str | None = None,
    ) -> None:
        super().__init__(
            *args,
            is_null=is_null,
            db_column_name=db_column_name,
            default=default,
            database_default=database_default,
        )


class TimeColumn(BaseDatetimeColumn[datetime.time]):
    """PostgreSQL type for `datetime.time` python type."""

    _available_comparison_types: tuple[
        type,
        ...,
    ] = (
        datetime.time,
        Column,
        All_,
        Any_,
    )
    _set_available_types: tuple[type, ...] = (datetime.time,)
    _sql_type = primitive_types.Time


class TimeTZColumn(BaseDateTimeColumnWithTZ[datetime.time]):
    """PostgreSQL type for `datetime.time` python type with TZ."""

    _available_comparison_types: tuple[
        type,
        ...,
    ] = (
        datetime.time,
        Column,
        All_,
        Any_,
    )
    _set_available_types: tuple[type, ...] = (datetime.time,)
    _sql_type = primitive_types.TimeTZ


class TimestampColumn(BaseDatetimeColumn[datetime.datetime]):
    """PostgreSQL type for `datetime.datetime` python type."""

    _available_comparison_types: tuple[
        type,
        ...,
    ] = (
        datetime.datetime,
        Column,
        All_,
        Any_,
    )
    _set_available_types: tuple[type, ...] = (datetime.datetime,)
    _sql_type = primitive_types.Timestamp


class TimestampTZColumn(BaseDateTimeColumnWithTZ[datetime.datetime]):
    """PostgreSQL type for `datetime.datetime` python type with TZ."""

    _available_comparison_types: tuple[
        type,
        ...,
    ] = (
        datetime.datetime,
        Column,
        All_,
        Any_,
    )
    _set_available_types: tuple[type, ...] = (datetime.datetime,)
    _sql_type = primitive_types.TimestampTZ


class IntervalColumn(Column[datetime.timedelta]):
    """PostgreSQL type for `datetime.timedelta` python type."""

    _available_comparison_types: tuple[
        type,
        ...,
    ] = (
        datetime.timedelta,
        Column,
        All_,
        Any_,
    )
    _set_available_types: tuple[type, ...] = (datetime.timedelta,)
    _sql_type = primitive_types.Interval
