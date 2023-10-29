import json
import types
from ast import literal_eval
from typing import Any, Dict, Final, List, Optional, Tuple, Union

from typing_extensions import Self

from qaspen.base.operators import AllOperator, AnyOperator
from qaspen.exceptions import FieldValueValidationError
from qaspen.fields.fields import Field
from qaspen.qaspen_types import FieldDefaultType, FieldType


# TODO: Add validate and converting default value to
# valid PostgreSQL value.
class JsonBase(Field[FieldType]):
    """Base field for JSON and JSONB PostgreSQL fields."""

    def __init__(
        self,
        *args: Any,
        is_null: bool = False,
        db_field_name: Optional[str] = None,
        default: FieldDefaultType[FieldType] = None,
    ) -> None:
        super().__init__(
            *args,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )

    def _prepare_default_value(self: Self) -> FieldDefaultType[FieldType]:
        """Prepare default value for PostgreSQL DEFAULT statement.

        ### Returns:
        `Any available type for this class`.
        """
        if type(self.default) == types.FunctionType:
            return self.default

        if isinstance(self.default, str):
            try:
                json.loads(self.default)
            except json.decoder.JSONDecodeError as exc:
                raise FieldValueValidationError(
                    f"Default value {self.default} of field "
                    f"{self.__class__.__name__} "
                    f"can't be serialized in PSQL {self._field_type} type.",
                ) from exc
            return f"'{self.default}'"  # type: ignore[return-value]

        if isinstance(self.default, (dict, list)):
            return self._dump_default(  # type: ignore[return-value]
                default_value=self.default,
            )

        if isinstance(self.default, bytes):
            return self._dump_default(  # type: ignore[return-value]
                literal_eval(
                    self.default.decode("utf-8"),
                ),
            )

        raise ValueError()

    def _dump_default(
        self: Self,
        default_value: Union[Dict[Any, Any], List[Any]],
    ) -> str:
        dump_value: Final = json.dumps(
            default_value,
            default=str,
        )
        return f"'{dump_value}'"


class Json(JsonBase[Union[Dict[Any, Any], str]]):
    """Field for JSON PostgreSQL type."""

    _available_comparison_types: Tuple[
        type,
        ...,
    ] = (
        dict,
        str,
        Field,
        AllOperator,
        AnyOperator,
    )
    _set_available_types: Tuple[type, ...] = (dict, str)


class Jsonb(JsonBase[Union[Dict[Any, Any], str, bytes]]):
    """Field for JSON PostgreSQL type."""

    _available_comparison_types: Tuple[
        type,
        ...,
    ] = (
        bytes,
        dict,
        str,
        Field,
        AllOperator,
        AnyOperator,
    )
    _set_available_types: Tuple[type, ...] = (dict, str, bytes)
