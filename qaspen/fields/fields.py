import typing
from qaspen.exceptions import StringFieldComparisonError

from qaspen.fields.base_field import Field
from qaspen.fields.comparisons import Where
from qaspen.fields.operators import GreaterOperator
from qaspen.fields.utils import validate_max_length


class BaseStringField(Field[str]):
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

    def _build_fields_sql_type(self: typing.Self) -> str:
        return f"{self._default_field_type}({self.max_length})"

    def __gt__(
        self: typing.Self,
        comparison_value: typing.Any,
    ) -> Where:
        if isinstance(comparison_value, str):
            return Where(
                field=self,
                compare_with_value=comparison_value,
                operator=GreaterOperator,
            )
        raise StringFieldComparisonError(
            f"It's impossible to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}"
        )

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
