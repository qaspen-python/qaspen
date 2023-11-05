from __future__ import annotations

import json
from ast import literal_eval
from typing import TYPE_CHECKING, Any, Dict, Final, List, Union

from qaspen.base.operators import AllOperator, AnyOperator
from qaspen.exceptions import FieldDeclarationError, FieldValueValidationError
from qaspen.fields.base import Field
from qaspen.qaspen_types import FieldDefaultType, FieldType
from qaspen.sql_type import complex_types

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.sql_type.base import SQLType


class JsonBase(Field[FieldType]):
    """Base field for JSON and JSONB PostgreSQL fields."""

    def __init__(
        self: Self,
        *args: Any,
        is_null: bool = False,
        db_field_name: str | None = None,
        default: FieldDefaultType[FieldType] = None,
    ) -> None:
        super().__init__(
            *args,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )

    def _prepare_default_value(
        self: Self,
        default_value: FieldType | None,
    ) -> str:
        """Prepare default value for PostgreSQL DEFAULT statement.

        ### Returns:
        `Any available type for this class`.
        """
        if isinstance(default_value, str):
            try:
                json.loads(default_value)
            except json.decoder.JSONDecodeError as exc:
                validation_err_msg: Final = (
                    f"Default value {default_value} of field "
                    f"{self.__class__.__name__} "
                    f"can't be serialized in PSQL {self._field_type} type.",
                )
                raise FieldValueValidationError(
                    validation_err_msg,
                ) from exc
            return f"'{default_value}'"

        if isinstance(default_value, (dict, list)):
            return self._dump_default(
                default_value=default_value,
            )

        if isinstance(default_value, bytes):
            return self._dump_default(
                literal_eval(
                    default_value.decode("utf-8"),
                ),
            )

        type_err_msg: Final = (
            f"Can't set default value {default_value} for "
            f"{self.__class__.__name__} field",
        )
        raise FieldDeclarationError(type_err_msg)

    def _dump_default(
        self: Self,
        default_value: dict[Any, Any] | list[Any],
    ) -> str:
        dump_value: Final = json.dumps(
            default_value,
            default=str,
        )
        return f"'{dump_value}'"


class JsonField(JsonBase[Union[Dict[Any, Any], str]]):
    """Field for JSON PostgreSQL type."""

    _available_comparison_types: tuple[
        type,
        ...,
    ] = (
        dict,
        list,
        str,
        Field,
        AllOperator,
        AnyOperator,
    )
    _set_available_types: tuple[type, ...] = (dict, list, str)
    _sql_type = complex_types.Json


class JsonbField(JsonBase[Union[Dict[Any, Any], str, bytes]]):
    """Field for JSON PostgreSQL type."""

    _available_comparison_types: tuple[
        type,
        ...,
    ] = (
        bytes,
        dict,
        str,
        list,
        Field,
        AllOperator,
        AnyOperator,
    )
    _set_available_types: tuple[type, ...] = (dict, str, bytes, list)
    _sql_type = complex_types.Jsonb


class ArrayField(Field[List[Any]]):
    """Field for ARRAY PostgreSQL type."""

    _available_comparison_types: tuple[
        type,
        ...,
    ] = (
        list,
        Field,
        AllOperator,
        AnyOperator,
    )
    _set_available_types: tuple[type, ...] = (list,)
    _sql_type = complex_types.Array

    def __init__(
        self: Self,
        *args: Any,
        base_type: type[SQLType],
        is_null: bool = False,
        db_field_name: str | None = None,
        default: FieldDefaultType[list[Any]] = None,
        dimension: int | None = None,
    ) -> None:
        super().__init__(
            *args,
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )

        self.dimension: Final = dimension
        self.base_type: Final = base_type

    def _prepare_default_value(
        self: Self,
        default_value: list[Any] | None,
    ) -> str | None:
        dumped_value = json.dumps(
            default_value,
            default=str,
        )
        return dumped_value.replace("[", "{").replace("]", "}")

    @property
    def _field_type(self: Self) -> str:
        sql_array_type: str = (
            f"{self.base_type.querystring()} {self._sql_type.sql_type()}"
        )
        if self.dimension:
            sql_array_type += f"[{self.dimension}]"

        return sql_array_type
