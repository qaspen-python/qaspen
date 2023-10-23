import datetime
from typing import Any, Callable, Optional, Tuple, Union

from typing_extensions import Final, Self

from qaspen.base.operators import AllOperator, AnyOperator
from qaspen.exceptions import FieldDeclarationError
from qaspen.fields.fields import Field


class DateField(Field[datetime.date]):
    _available_comparison_types: Tuple[
        type,
        ...,
    ] = (
        datetime.date,
        Field,
        AllOperator,
        AnyOperator,
    )
    _set_available_types: tuple[type, ...] = (datetime.date,)
    _database_default: str = "CURRENT_DATE"

    def __init__(
        self,
        *args: Any,
        is_null: bool = False,
        default: Union[
            datetime.date,
            Callable[[], Union[datetime.date, datetime.datetime]],
            None,
        ] = None,
        db_field_name: Optional[str] = None,
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
        if not self.database_default:
            return super()._field_default
        return f"DEFAULT {self._database_default}"
