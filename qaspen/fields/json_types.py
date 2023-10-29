from typing import Any, Dict, Optional, Tuple, Union

from qaspen.base.operators import AllOperator, AnyOperator
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
