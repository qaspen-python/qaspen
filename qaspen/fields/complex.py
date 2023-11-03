import json
from ast import literal_eval
from typing import Any, Dict, Final, List, Optional, Tuple, Type, Union

from typing_extensions import Self

from qaspen.base.operators import AllOperator, AnyOperator
from qaspen.exceptions import FieldDeclarationError, FieldValueValidationError
from qaspen.fields.base import Field
from qaspen.qaspen_types import FieldDefaultType, FieldType
from qaspen.sql_type import complex_types
from qaspen.sql_type.base import SQLType


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

    def _prepare_default_value(
        self: Self,
        default_value: Optional[FieldType],
    ) -> str:
        """Prepare default value for PostgreSQL DEFAULT statement.

        ### Returns:
        `Any available type for this class`.
        """
        if isinstance(default_value, str):
            try:
                json.loads(default_value)
            except json.decoder.JSONDecodeError as exc:
                raise FieldValueValidationError(
                    f"Default value {default_value} of field "
                    f"{self.__class__.__name__} "
                    f"can't be serialized in PSQL {self._field_type} type.",
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

        raise FieldDeclarationError(
            f"Can't set default value {default_value} for "
            f"{self.__class__.__name__} field",
        )

    def _dump_default(
        self: Self,
        default_value: Union[Dict[Any, Any], List[Any]],
    ) -> str:
        dump_value: Final = json.dumps(
            default_value,
            default=str,
        )
        return f"'{dump_value}'"


class JsonField(JsonBase[Union[Dict[Any, Any], str]]):
    """Field for JSON PostgreSQL type."""

    _available_comparison_types: Tuple[
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
    _set_available_types: Tuple[type, ...] = (dict, list, str)
    _sql_type = complex_types.Json


class JsonbField(JsonBase[Union[Dict[Any, Any], str, bytes]]):
    """Field for JSON PostgreSQL type."""

    _available_comparison_types: Tuple[
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
    _set_available_types: Tuple[type, ...] = (dict, str, bytes, list)
    _sql_type = complex_types.Jsonb


class ArrayField(Field[List[Any]]):
    """Field for ARRAY PostgreSQL type."""

    _available_comparison_types: Tuple[
        type,
        ...,
    ] = (
        list,
        Field,
        AllOperator,
        AnyOperator,
    )
    _set_available_types: Tuple[type, ...] = (list,)
    _sql_type = complex_types.Array

    def __init__(
        self,
        *args: Any,
        base_type: Type[SQLType],
        is_null: bool = False,
        db_field_name: Optional[str] = None,
        default: FieldDefaultType[List[Any]] = None,
        dimension: Optional[int] = None,
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
        default_value: Optional[List[Any]],
    ) -> Optional[str]:
        dumped_value = json.dumps(
            default_value,
            default=str,
        )
        dumped_value = dumped_value.replace("[", "{").replace("]", "}")
        return dumped_value

    @property
    def _field_type(self: Self) -> str:
        sql_array_type: str = (
            f"{self.base_type.querystring()} " f"{self._sql_type.sql_type()}"
        )
        if self.dimension:
            sql_array_type += f"[{self.dimension}]"

        return sql_array_type
