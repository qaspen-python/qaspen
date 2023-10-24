from typing import TYPE_CHECKING, Any, Optional, Tuple, overload

from typing_extensions import Self

from qaspen.base.operators import AllOperator, AnyOperator
from qaspen.exceptions import FieldComparisonError, FieldValueValidationError
from qaspen.fields import operators
from qaspen.fields.fields import Field
from qaspen.fields.utils import validate_max_length
from qaspen.statements.combinable_statements.filter_statement import Filter

if TYPE_CHECKING:
    pass


AvailableComparisonTypes = (
    str,
    Field,
    AnyOperator,
    AllOperator,
)


class BaseStringField(Field[str]):
    _available_comparison_types: Tuple[type, ...] = AvailableComparisonTypes
    _set_available_types: Tuple[type, ...] = (str,)

    @overload
    def __init__(
        self: Self,
        max_length: int = 255,
        is_null: bool = False,
        default: Optional[str] = None,
        db_field_name: Optional[str] = None,
    ) -> None:
        ...

    @overload
    def __init__(
        self: Self,
        is_null: bool = False,
        default: Optional[str] = None,
        db_field_name: Optional[str] = None,
    ) -> None:
        ...

    def __init__(
        self: Self,
        *args: Any,
        max_length: int = 255,
        is_null: bool = False,
        default: Optional[str] = None,
        db_field_name: Optional[str] = None,
    ) -> None:
        if max_length:
            validate_max_length(max_length=max_length)

        self._max_length: int = max_length

        super().__init__(
            *args,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )

    def like(
        self: Self,
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
        self: Self,
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
        self: Self,
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
        self: Self,
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
        self: Self,
        field_value: Optional[str],
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

    @property
    def _sql_type(self: Self) -> str:
        return f"{self._field_type}({self._max_length})"


class VarChar(BaseStringField):
    """Varchar Field.

    Behave like normal PostgreSQL VARCHAR field.
    """


class Text(Field[str]):
    """Text field.

    Behave like normal PostgreSQL TEXT field.
    """

    _available_comparison_types: Tuple[type, ...] = AvailableComparisonTypes
    _set_available_types: Tuple[type, ...] = (str,)


class Char(Field[str]):
    """Char field.

    You cannot specify `max_length` parameter for this Field,
    it's always 1.

    If you want more characters, use `VarChar` field.
    """

    _available_comparison_types: Tuple[type, ...] = AvailableComparisonTypes
    _set_available_types: Tuple[type, ...] = (str,)

    def __init__(
        self: Self,
        *pos_arguments: Any,
        is_null: bool = False,
        default: Optional[str] = None,
        db_field_name: Optional[str] = None,
    ) -> None:
        super().__init__(
            *pos_arguments,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )

    def _validate_field_value(
        self: Self,
        field_value: Optional[str],
    ) -> None:
        """Validate field value.

        If value length not equal 1 raise an error.

        :param field_value: new value for the field.

        :raises FieldValueValidationError: if value length not equal 1.
        """
        if not field_value or len(field_value) == 1:
            return

        raise FieldValueValidationError(
            f"CHAR field must always contain "
            f"only one character. "
            f"You tried to set {field_value}",
        )
