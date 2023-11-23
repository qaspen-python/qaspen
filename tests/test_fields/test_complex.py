from __future__ import annotations

from typing import Any

import pytest

from qaspen.exceptions import FieldDeclarationError, FieldValueValidationError
from qaspen.fields.complex import ArrayField, JsonBase, JsonbField, JsonField
from qaspen.sql_type.primitive_types import VarChar


@pytest.mark.parametrize(
    (
        "field",
        "raw_default_value",
        "prepared_default_value",
        "expected_exception",
    ),
    [
        (JsonField, {"qaspen": 1000}, """'{"qaspen": 1000}'""", None),
        (
            JsonField,
            {"qaspen": 1000, "qas": {1: "123"}},
            """'{"qaspen": 1000, "qas": {"1": "123"}}'""",
            None,
        ),
        (JsonField, [1, "123"], """'[1, "123"]'""", None),
        (
            JsonField,
            [1, "123", {"qaspen": 123}],
            """'[1, "123", {"qaspen": 123}]'""",
            None,
        ),
        (
            JsonField,
            '{"qaspen": "awesome"}',
            """'{"qaspen": "awesome"}'""",
            None,
        ),
        (
            JsonField,
            "just string, isn't correct.",
            None,
            FieldValueValidationError,
        ),
        (JsonbField, {"qaspen": 1000}, """'{"qaspen": 1000}'""", None),
        (
            JsonbField,
            {"qaspen": 1000, "qas": {1: "123"}},
            """'{"qaspen": 1000, "qas": {"1": "123"}}'""",
            None,
        ),
        (JsonbField, [1, "123"], """'[1, "123"]'""", None),
        (
            JsonbField,
            [1, "123", {"qaspen": 123}],
            """'[1, "123", {"qaspen": 123}]'""",
            None,
        ),
        (
            JsonbField,
            '{"qaspen": "awesome"}',
            """'{"qaspen": "awesome"}'""",
            None,
        ),
        (
            JsonbField,
            b'{"qaspen": "awesome"}',
            """'{"qaspen": "awesome"}'""",
            None,
        ),
        (
            JsonbField,
            "just string, isn't correct.",
            None,
            FieldValueValidationError,
        ),
    ],
)
def test_json_field_default_value(
    field: type[JsonBase[Any]],
    raw_default_value: Any,
    prepared_default_value: str,
    expected_exception: type[Exception] | None,
) -> None:
    """Test `default` and `_prepare_default_value` method for json fields.

    Check that strings can be loaded to json, dict, list and bytes
    convert into correct string.
    """
    if expected_exception:
        with pytest.raises(expected_exception=expected_exception):
            field(default=raw_default_value)
    else:
        created_field = field(default=raw_default_value)
        assert created_field._default == prepared_default_value


@pytest.mark.parametrize(
    "field",
    [JsonField, JsonbField],
)
@pytest.mark.parametrize(
    "default_value",
    [12, 12.0, {1, 2, 3}, frozenset([1, 2, 3])],
)
def test_json_field_prepare_default_value_failure(
    field: type[JsonBase[Any]],
    default_value: Any,
) -> None:
    """Test json field `_prepare_default_value` method.

    Check that it fails if give it wrong type.
    """
    created_field = field()
    with pytest.raises(expected_exception=FieldDeclarationError):
        created_field._prepare_default_value(default_value=default_value)


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
def test_array_field_prepare_default_value_method(
    raw_default_value: list[Any],
    prepared_default_value: str,
) -> None:
    """Test `_prepare_default_value` `ArrayField` method."""
    created_field = ArrayField(
        base_type=VarChar,
        default=raw_default_value,
    )
    assert created_field._default == prepared_default_value


def test_array_field_field_type_method() -> None:
    """Test `_field_type` method."""
    field_without_dimension = ArrayField(
        base_type=VarChar,
    )
    assert field_without_dimension._field_type == "VARCHAR ARRAY"

    field_with_dimension = ArrayField(
        base_type=VarChar,
        dimension=10,
    )
    assert field_with_dimension._field_type == "VARCHAR ARRAY[10]"
