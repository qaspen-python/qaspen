import datetime
from typing import Any, Optional, Tuple

from typing_extensions import Final, Self

from qaspen.base.operators import AllOperator, AnyOperator
from qaspen.exceptions import FieldDeclarationError
from qaspen.fields.base_field import FieldDefaultType, FieldType
from qaspen.fields.fields import Field


class BaseDatetimeField(Field[FieldType]):
    """Base Field for all Date/Time fields."""

    _database_default: str = "CURRENT_DATE"

    def __init__(
        self,
        *args: Any,
        is_null: bool = False,
        db_field_name: Optional[str] = None,
        default: FieldDefaultType[FieldType] = None,
        database_default: bool = False,
    ) -> None:
        if default and database_default:
            raise FieldDeclarationError(
                "Please specify either `default` or `database_default` "
                "for DateField.",
            )

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
        self,
        *args: Any,
        is_null: bool = False,
        db_field_name: Optional[str] = None,
        default: FieldDefaultType[FieldType] = None,
        database_default: bool = False,
        with_timezone: bool = False,
    ) -> None:
        if default and database_default:
            raise FieldDeclarationError(
                "Please specify either `default` or `database_default` "
                "for DateField.",
            )

        super().__init__(
            *args,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
            database_default=database_default,
        )

        self.with_timezone: Final = with_timezone

    @property
    def _sql_type(self: Self) -> str:
        if self.with_timezone:
            return f"{self._default_field_type} WITH TIME ZONE"
        return self._default_field_type


class Date(BaseDatetimeField[datetime.date]):
    """PostgreSQL type for `datetime.date` python type."""

    _available_comparison_types: Tuple[
        type,
        ...,
    ] = (
        datetime.date,
        Field,
        AllOperator,
        AnyOperator,
    )
    _set_available_types: Tuple[type, ...] = (datetime.date,)


class Time(BaseDateTimeFieldWithTZ[datetime.time]):
    """PostgreSQL type for `datetime.time` python type."""

    _database_default: str = "CURRENT_TIME"

    _available_comparison_types: Tuple[
        type,
        ...,
    ] = (
        datetime.time,
        Field,
        AllOperator,
        AnyOperator,
    )
    _set_available_types: Tuple[type, ...] = (datetime.time,)


class Timestamp(BaseDateTimeFieldWithTZ[datetime.datetime]):
    """PostgreSQL type for `datetime.datetime` python type."""

    _available_comparison_types: Tuple[
        type,
        ...,
    ] = (
        datetime.datetime,
        Field,
        AllOperator,
        AnyOperator,
    )
    _set_available_types: Tuple[type, ...] = (datetime.datetime,)


class Interval(Field[datetime.timedelta]):
    """PostgreSQL type for `datetime.timedelta` python type."""

    _available_comparison_types: Tuple[
        type,
        ...,
    ] = (
        datetime.timedelta,
        Field,
        AllOperator,
        AnyOperator,
    )
    _set_available_types: Tuple[type, ...] = (datetime.timedelta,)
