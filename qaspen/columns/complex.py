from __future__ import annotations

import json
from ast import literal_eval
from typing import TYPE_CHECKING, Any, Dict, Final, List, Union

from qaspen.base.operators import All_, Any_
from qaspen.columns.base import Column
from qaspen.exceptions import (
    ColumnDeclarationError,
    ColumnValueValidationError,
)
from qaspen.qaspen_types import ColumnDefaultType, ColumnType
from qaspen.sql_type import complex_types

if TYPE_CHECKING:
    from typing_extensions import Self


class JsonBase(Column[ColumnType]):
    """Base column for JSON and JSONB PostgreSQL columns."""

    def __init__(
        self: Self,
        *args: Any,
        is_null: bool = True,
        db_column_name: str | None = None,
        default: ColumnDefaultType[ColumnType] = None,
        database_default: str | None = None,
    ) -> None:
        super().__init__(
            *args,
            is_null=is_null,
            default=default,
            database_default=database_default,
            db_column_name=db_column_name,
        )

    def _prepare_default_value(
        self: Self,
        default_value: ColumnType | None,
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
                    f"Default value {default_value} of column "
                    f"{self.__class__.__name__} "
                    f"can't be serialized in PSQL {self._column_type} type.",
                )
                raise ColumnValueValidationError(
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
            f"{self.__class__.__name__} column",
        )
        raise ColumnDeclarationError(type_err_msg)

    def _dump_default(
        self: Self,
        default_value: dict[Any, Any] | list[Any],
    ) -> str:
        dump_value: Final = json.dumps(
            default_value,
            default=str,
        )
        return f"'{dump_value}'"


class JsonColumn(JsonBase[Union[Dict[Any, Any], str]]):
    """Column for JSON PostgreSQL type."""

    _available_comparison_types: tuple[
        type,
        ...,
    ] = (
        dict,
        list,
        str,
        Column,
        All_,
        Any_,
    )
    _set_available_types: tuple[type, ...] = (dict, list, str)
    _sql_type = complex_types.Json

    def __init__(
        self: Self,
        *args: Any,
        is_null: bool = True,
        db_column_name: str | None = None,
        database_default: str | None = None,
        default: ColumnDefaultType[dict[Any, Any] | str] = None,
    ) -> None:
        super().__init__(
            *args,
            is_null=is_null,
            default=default,
            database_default=database_default,
            db_column_name=db_column_name,
        )


class JsonbColumn(JsonBase[Union[Dict[Any, Any], str, bytes]]):
    """Column for JSON PostgreSQL type."""

    _available_comparison_types: tuple[
        type,
        ...,
    ] = (
        bytes,
        dict,
        str,
        list,
        Column,
        All_,
        Any_,
    )
    _set_available_types: tuple[type, ...] = (dict, str, bytes, list)
    _sql_type = complex_types.Jsonb

    def __init__(
        self: Self,
        *args: Any,
        is_null: bool = True,
        db_column_name: str | None = None,
        database_default: str | None = None,
        default: ColumnDefaultType[dict[Any, Any] | str | bytes] = None,
    ) -> None:
        super().__init__(
            *args,
            is_null=is_null,
            default=default,
            database_default=database_default,
            db_column_name=db_column_name,
        )


class ArrayColumn(Column[List[Any]]):
    """Column for ARRAY PostgreSQL type."""

    _available_comparison_types: tuple[
        type,
        ...,
    ] = (
        list,
        Column,
        All_,
        Any_,
    )
    _set_available_types: tuple[type, ...] = (list,)
    _sql_type = complex_types.Array

    def __init__(
        self: Self,
        *args: Any,
        inner_column: Column[Any],
        is_null: bool = True,
        db_column_name: str | None = None,
        default: ColumnDefaultType[list[Any]] = None,
        database_default: str | None = None,
        dimension: int | None = None,
    ) -> None:
        if isinstance(inner_column, ArrayColumn):
            exception_message: Final = (
                "Nested arrays are not allowed.\n",
                "Please, use JSONColumn or JSONBColumn instead",
            )
            raise ColumnDeclarationError(exception_message)
        super().__init__(
            *args,
            is_null=is_null,
            default=default,
            database_default=database_default,
            db_column_name=db_column_name,
        )

        self.dimension: Final = dimension
        self.inner_column: Final = inner_column

    def _prepare_default_value(
        self: Self,
        default_value: list[Any] | None,
    ) -> str | None:
        dumped_value = json.dumps(
            default_value,
            default=str,
        )
        dumped_value = dumped_value.replace("[", "{").replace("]", "}")
        return f"'{dumped_value}'"

    @property
    def _column_type(self: Self) -> str:
        if self.dimension:
            return f"{self.inner_column._column_type}[{self.dimension}]"

        return f"{self.inner_column._column_type}[]"
