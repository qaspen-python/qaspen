from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

import pytest

from qaspen.sql_type.mapper import map_python_type_to_sql
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


class ForTestClass:
    """Just for test."""


@pytest.mark.parametrize(
    ("python_type", "sql_type"),
    [
        ("local", VarChar),
        (12, BigInt),
        (5.0, Decimal),
        (datetime.datetime.now().date(), Date),  # noqa: DTZ005
        (datetime.datetime.now().time(), Time),  # noqa: DTZ005
        (datetime.datetime.now(), Timestamp),  # noqa: DTZ005
        (ForTestClass(), None),
    ],
)
def test_map_python_type_to_sql_type(
    python_type: type,
    sql_type: type[SQLType],
) -> None:
    """Test map from python type to SQL."""
    assert (
        map_python_type_to_sql(
            python_type,
        )
        == sql_type
    )
