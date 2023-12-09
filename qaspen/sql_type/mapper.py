from __future__ import annotations

from datetime import date, datetime, time
from typing import TYPE_CHECKING, Any

from qaspen.sql_type.primitive_types import (
    BigInt,
    Date,
    Decimal,
    Time,
    Timestamp,
    VarChar,
)

if TYPE_CHECKING:
    from qaspen.sql_type.base import SQLType


def map_python_type_to_sql(  # noqa: PLR0911
    for_match_value: Any,
) -> type[SQLType] | None:
    """Map python type to SQL type.

    ### Returns:
    subclass of `SQLType`.
    """
    if isinstance(for_match_value, str):
        return VarChar
    if isinstance(for_match_value, int):
        return BigInt
    if isinstance(for_match_value, float):
        return Decimal
    if isinstance(for_match_value, datetime):
        return Timestamp
    if isinstance(for_match_value, date):
        return Date
    if isinstance(for_match_value, time):
        return Time

    return None
