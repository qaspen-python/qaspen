from __future__ import annotations

from typing import Any

import pytest

from qaspen.columns.complex import (
    ArrayColumn,
    JsonBase,
    JsonbColumn,
    JsonColumn,
)
from qaspen.columns.primitive import VarCharColumn
from qaspen.exceptions import (
    ColumnDeclarationError,
    ColumnValueValidationError,
)


@pytest.mark.parametrize(
    (
        "column",
        "raw_default_value",
        "prepared_default_value",
        "expected_exception",
    ),
    [
        (JsonColumn, {"qaspen": 1000}, """'{"qaspen": 1000}'""", None),
        (
            JsonColumn,
            {"qaspen": 1000, "qas": {1: "123"}},
            """'{"qaspen": 1000, "qas": {"1": "123"}}'""",
            None,
        ),
        (JsonColumn, [1, "123"], """'[1, "123"]'""", None),
        (
            JsonColumn,
            [1, "123", {"qaspen": 123}],
            """'[1, "123", {"qaspen": 123}]'""",
            None,
        ),
        (
            JsonColumn,
            '{"qaspen": "awesome"}',
            """'{"qaspen": "awesome"}'""",
            None,
        ),
        (
            JsonColumn,
            "just string, isn't correct.",
            None,
            ColumnValueValidationError,
        ),
        (JsonbColumn, {"qaspen": 1000}, """'{"qaspen": 1000}'""", None),
        (
            JsonbColumn,
            {"qaspen": 1000, "qas": {1: "123"}},
            """'{"qaspen": 1000, "qas": {"1": "123"}}'""",
            None,
        ),
        (JsonbColumn, [1, "123"], """'[1, "123"]'""", None),
        (
            JsonbColumn,
            [1, "123", {"qaspen": 123}],
            """'[1, "123", {"qaspen": 123}]'""",
            None,
        ),
        (
            JsonbColumn,
            '{"qaspen": "awesome"}',
            """'{"qaspen": "awesome"}'""",
            None,
        ),
        (
            JsonbColumn,
            b'{"qaspen": "awesome"}',
            """'{"qaspen": "awesome"}'""",
            None,
        ),
        (
            JsonbColumn,
            "just string, isn't correct.",
            None,
            ColumnValueValidationError,
        ),
    ],
)
def test_json_column_default_value(
    column: type[JsonBase[Any]],
    raw_default_value: Any,
    prepared_default_value: Any,
    expected_exception: type[Exception] | None,
) -> None:
    """Test `default` and `_prepare_default_value` method for json columns.

    Check that strings can be loaded to json, dict, list and bytes
    convert into correct string.
    """
    if expected_exception:
        with pytest.raises(expected_exception=expected_exception):
            column(default=raw_default_value)
    else:
        created_column = column(default=raw_default_value)
        assert created_column._prepared_default == prepared_default_value


@pytest.mark.parametrize(
    "column",
    [JsonColumn, JsonbColumn],
)
@pytest.mark.parametrize(
    "default_value",
    [12, 12.0, {1, 2, 3}, frozenset([1, 2, 3])],
)
def test_json_column_prepare_default_value_failure(
    column: type[JsonBase[Any]],
    default_value: Any,
) -> None:
    """Test json column `_prepare_default_value` method.

    Check that it fails if give it wrong type.
    """
    created_column = column()
    with pytest.raises(expected_exception=ColumnDeclarationError):
        created_column._prepare_default_value(default_value=default_value)


@pytest.mark.parametrize(
    ("raw_default_value", "prepared_default_value"),
    [
        (
            [1, 2, "3"],
            """'{1, 2, "3"}'""",
        ),
        (
            [1, 2, "3", [14, "qaspen"]],
            """'{1, 2, "3", {14, "qaspen"}}'""",
        ),
    ],
)
def test_array_column_prepare_default_value_method(
    raw_default_value: list[Any],
    prepared_default_value: str,
) -> None:
    """Test `_prepare_default_value` `ArrayColumn` method."""
    created_column = ArrayColumn(
        inner_column=VarCharColumn(),
        default=raw_default_value,
    )
    assert created_column._prepared_default == prepared_default_value


def test_array_column_column_type_method() -> None:
    """Test `_column_type` method."""
    column_without_dimension = ArrayColumn(
        inner_column=VarCharColumn(max_length=64),
    )
    assert column_without_dimension._column_type == "VARCHAR(64)[]"

    column_with_dimension = ArrayColumn(
        inner_column=VarCharColumn(),
        dimension=10,
    )
    assert column_with_dimension._column_type == "VARCHAR(255)[10]"


def test_array_column_nested_arrays() -> None:
    """Test nested arrays are not allowed."""
    with pytest.raises(ColumnDeclarationError):
        ArrayColumn(
            inner_column=ArrayColumn(
                inner_column=VarCharColumn(max_length=64),
            ),
        )
