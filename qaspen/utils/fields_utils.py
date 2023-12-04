import json
import typing
from enum import Enum


def transform_value_to_sql(  # noqa: PLR0911
    value_to_convert: typing.Any,
) -> str:
    """Convert python value to SQL string.

    ### Parameters:
    - `value_to_convert`: value to convert into SQL string.

    ### Returns:
    SQL string.
    """
    if value_to_convert is None:
        return "NULL"
    if isinstance(value_to_convert, str):
        return f"'{value_to_convert}'"
    if isinstance(value_to_convert, (int, float)):
        return str(value_to_convert)
    if isinstance(value_to_convert, complex):
        return str(value_to_convert).replace("(", "").replace(")", "")
    if isinstance(value_to_convert, Enum):
        return transform_value_to_sql(
            value_to_convert.value,
        )
    if isinstance(value_to_convert, dict):
        return json.dumps(
            value_to_convert,
            default=str,
            indent=2,
        )
    return str(value_to_convert)
