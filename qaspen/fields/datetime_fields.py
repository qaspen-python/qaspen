import datetime
import typing

from qaspen.base.operators import AllOperator, AnyOperator
from qaspen.fields.fields import Field


class DateField(Field[datetime.date]):
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

    def __init__(
        self,
        *args: typing.Any,
        is_null: bool = False,
        default: typing.Union[
            datetime.date,
            typing.Callable[
                [],
                typing.Union[datetime.date, datetime.datetime],
            ],
            None,
        ] = None,
        db_field_name: str | None = None,
    ) -> None:
        super().__init__(
            *args,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )
