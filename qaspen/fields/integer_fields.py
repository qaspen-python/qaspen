from typing import Any, Final, Optional, Tuple, Union

from typing_extensions import Self

from qaspen.base.operators import AllOperator, AnyOperator
from qaspen.exceptions import FieldDeclarationError, FieldValueValidationError
from qaspen.fields.fields import Field


class BaseIntegerField(Field[int]):
    """Base field for all integer fields."""

    _available_comparison_types: Tuple[type, ...] = (
        int,
        Field,
        AnyOperator,
        AllOperator,
    )
    _set_available_types: Tuple[type, ...] = (int,)
    _available_max_value: Optional[int]
    _available_min_value: Optional[int]

    def __init__(
        self: Self,
        *pos_arguments: Any,
        is_null: bool = False,
        default: Optional[int] = None,
        db_field_name: Optional[str] = None,
        maximum: Optional[int] = None,
        minimum: Optional[int] = None,
    ) -> None:
        # TODO: Added CHECK constraint to these params
        self._maximum: Optional[int] = maximum
        self._minimum: Optional[int] = minimum

        super().__init__(
            *pos_arguments,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )

    def _validate_field_value(
        self: Self,
        field_value: Optional[int],
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
            raise FieldValueValidationError(
                f"Field value - `{field_value}` "
                f"is bigger than field `{self.__class__.__name__}` "
                f"can accommodate - `{self._available_max_value}`",
            )

        is_min_value_reached: Final = bool(
            field_value
            and self._available_min_value
            and field_value < self._available_min_value,
        )
        if is_min_value_reached:
            raise FieldValueValidationError(
                f"Field value - `{field_value}` "
                f"is less than field `{self.__class__.__name__}` "
                f"can accommodate - `{self._available_min_value}`",
            )
        if field_value and self._maximum and field_value > self._maximum:
            raise FieldValueValidationError(
                f"Field value - `{field_value}` is bigger than maximum you set `{self._maximum}`",
            )
        if field_value and self._minimum and field_value < self._minimum:
            raise FieldValueValidationError(
                f"Field value - `{field_value}` is less than minimum you set `{self._minimum}`",
            )


class SmallInt(BaseIntegerField):
    """SMALLINT field."""

    _available_max_value: int = 32767
    _available_min_value: int = -32768


class Integer(BaseIntegerField):
    """INTEGER field."""

    _available_max_value: int = 2147483647
    _available_min_value: int = -2147483648


class Boolean(Field[bool]):
    """BOOLEAN field."""

    _set_available_types: Tuple[type, ...] = (bool,)

    def _validate_field_value(
        self: Self,
        field_value: Optional[bool],
    ) -> None:
        """Validate field value.

        Check all possible BOOLEAN field values.

        ### Parameters
        - field_value: field to validate

        ### Returns
        - `None`
        """
        super()._validate_field_value(
            field_value=field_value,
        )

        if field_value not in (True, False, None):
            raise FieldValueValidationError(
                f"Field value - `{field_value}` must be one of the following: True, False, or None",
            )


class BigInt(BaseIntegerField):
    """BIGINT field."""

    _available_max_value: int = 9223372036854775807
    _available_min_value: int = -9223372036854775808


class Numeric(BaseIntegerField):
    """NUMERIC field."""

    def __init__(
        self: Self,
        *pos_arguments: Any,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
        is_null: bool = False,
        default: Optional[int] = None,
        db_field_name: Optional[str] = None,
        maximum: Optional[int] = None,
        minimum: Optional[int] = None,
    ) -> None:
        if not precision and scale:
            raise FieldDeclarationError(
                "You cannot specify `scale` without `precision`.",
            )

        super().__init__(
            *pos_arguments,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
            maximum=maximum,
            minimum=minimum,
        )

        self.precision: Optional[int] = precision
        self.scale: Optional[int] = scale

    @property
    def _sql_type(self: Self) -> str:
        field_type: str = self._field_type
        if self.precision and self.scale:
            field_type += f"({self.precision}, {self.scale})"
        elif self.precision:
            field_type += f"({self.precision})"

        return field_type


class Decimal(Numeric):
    """DECIMAL field.

    The same as `Numeric` field.
    """


class Real(Field[Union[int, str]]):
    """REAL field."""

    def __init__(
        self: Self,
        *pos_arguments: Any,
        is_null: bool = False,
        default: Union[int, str, None] = None,
        db_field_name: Optional[str] = None,
    ) -> None:
        super().__init__(
            *pos_arguments,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )


class DoublePrecision(Field[Union[int, str]]):
    """DOUBLE PRECISION field."""

    def __init__(
        self: Self,
        *pos_arguments: Any,
        is_null: bool = False,
        default: Union[int, str, None] = None,
        db_field_name: Optional[str] = None,
    ) -> None:
        super().__init__(
            *pos_arguments,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )


class SerialBaseField(BaseIntegerField):
    """Base Serial field for all possible SERIAL fields."""

    _sub_field: str

    def __init__(
        self: Self,
        *pos_arguments: Any,
        db_field_name: Optional[str] = None,
        maximum: Optional[int] = None,
        minimum: Optional[int] = None,
        next_val_seq_name: Optional[str] = None,
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

        self.next_val_seq_name: Optional[str] = next_val_seq_name

    def _make_field_create_statement(self: Self) -> str:
        if self.next_val_seq_name:
            return f"{self.original_field_name} {self._sub_field} NOT NULL DEFAULT nextval('{self.next_val_seq_name}')"
        return f"{self.original_field_name} {self._sql_type}"


class SmallSerial(SerialBaseField, SmallInt):
    """SMALLSERIAL field.

    Its `SmallInt` field with `NOT NULL` property,
    and autoincrement functionality.
    """

    _sub_field: str = "SMALLINT"


class Serial(SerialBaseField, Integer):
    """SERIAL field.

    Its `Integer` field with `NOT NULL` property,
    and autoincrement functionality.
    """

    _sub_field: str = "INTEGER"


class BigSerial(SerialBaseField, BigInt):
    """BIGSERIAL field.

    Its `BigInt` field with `NOT NULL` property,
    and autoincrement functionality.
    """

    _sub_field: str = "BIGINT"
