import abc
import dataclasses
import types
from typing import TYPE_CHECKING, Any, Generic, Optional, Type, Union, final

from typing_extensions import Self

from qaspen.qaspen_types import FieldDefaultType, FieldType

if TYPE_CHECKING:
    from qaspen.table.base_table import BaseTable


class EmptyFieldValue:
    """Indicates that field wasn't queried from the database."""

    def __str__(self: Self) -> str:
        return self.__class__.__name__


EMPTY_FIELD_VALUE = EmptyFieldValue()


@dataclasses.dataclass
class FieldData(Generic[FieldType]):
    """All field data.

    ### Fields
    `field_name` - Name of the field

    `from_table` - From what table field is.

    `is_null` - is field can be NULL.

    `field_value` - value of the field in the database or
    if you create table instance with fields manually.

    `default` - default value for the field.
    If it is callable, do not use it in the database,
    use it in python and than save in the database.

    `prefix` - prefix for the field in the query.

    `alias` - alias of the field.

    `in_join` - mark that field is used in join.
    """

    field_name: str
    from_table: Type["BaseTable"] = None  # type: ignore[assignment]
    is_null: bool = False
    field_value: Union[FieldType, EmptyFieldValue, None] = EMPTY_FIELD_VALUE
    default: FieldDefaultType[FieldType] = None
    prefix: str = ""
    alias: str = ""
    in_join: bool = False


class BaseField(Generic[FieldType], abc.ABC):
    """Base field class for all Fields."""

    _field_data: FieldData[FieldType]

    def __set_name__(
        self: Self,
        owner: Any,
        field_name: str,
    ) -> None:
        """Set name for the field.

        field_name is equal to the name of the Field variable.

        Example:
        -------
        ```
        class MyTable(BaseTable):
            # name of the field in the database is `meme_lord`
            meme_lord: VarCharField = VarCharField()
        ```
        """
        self._field_data.from_table = owner
        self._field_data.field_name = field_name

    @abc.abstractmethod
    def _make_field_create_statement(
        self: Self,
    ) -> str:
        ...

    @abc.abstractmethod
    def _validate_field_value(
        self: Self,
        field_value: Optional[FieldType],
    ) -> None:
        """Validate field value.

        It must raise an error if something goes wrong
        or not return anything.
        """
        ...

    @abc.abstractmethod
    def _validate_default_value(
        self: Self,
        default_value: Optional[FieldType],
    ) -> None:
        """Validate field default value.

        It must raise an error in validation failed.
        """
        ...

    @property
    def value(self: Self) -> Union[FieldType, EmptyFieldValue, None]:
        """Return value of the field.

        It's valid only for fields after executing statement,
        otherwise it returns `None`.

        If you don't select field in the `.select()` and
        try to get its value after `as_objects()` method,
        you will retrieve `EmptyFieldValue` because it indicates
        that you don't select this Field.

        ### Returns:
        `Python type of the field` or `EmptyFieldValue` or `None`.
        """
        return self._field_data.field_value

    @property
    def _table_name(self: Self) -> str:
        """Return table name of this field.

        It's only the original table name, not aliased.

        ### Return
        `str` table name.
        """
        return self._field_data.from_table.original_table_name()

    @property
    def _original_field_name(self: Self) -> str:
        """Return name of the field without prefix and alias.

        ### Return
        `str` Field name.
        """
        return self._field_data.field_name

    @property
    def field_name(self: Self) -> str:
        """Return field name with prefix and alias.

        ### Prefix logic:
        If `from_table` has alias than use this alias.

        If this field has a prefix than use prefix.

        Else use original name of the field's Table.

        ### Alias logic:
        If this Field has an alias, use it.

        ### Return
        `Field` as a `str`.
        """
        prefix: str = (
            self._field_data.from_table._table_meta.alias
            or self._field_data.prefix
            or self._field_data.from_table.table_name()
        )
        field_name: str = f"{prefix}.{self._field_data.field_name}"
        if alias := self._field_data.alias:
            field_name += f" AS {alias}"

        return field_name

    @property
    def _default(self: Self) -> FieldDefaultType[FieldType]:
        """Return default value of the field.

        ### Return
        default value.
        """
        return self._field_data.default

    @property
    def _is_null(self: Self) -> bool:
        """Return flag that field can be `NULL`.

        ### Return
        `bool`
        """
        return self._field_data.is_null

    @property
    def _field_null(self: Self) -> str:
        """Return `NOT NULL` string if field cannot be NULL.

        ### Return
        `str`.
        """
        return "NOT NULL" if not self._is_null else ""

    @property
    def _field_default(self: Self) -> str:
        """Return SQL string for DEFAULT value for a Field.

        ### Return
        `str`
        """
        if self._default and not types.FunctionType == type(self._default):
            return f"DEFAULT {self._default}" if self._default else ""
        return ""

    @property
    @final
    def _field_type(self: Self) -> str:
        """Field type in the SQL."""
        return self.__class__.__name__.upper()

    @property
    def _sql_type(self: Self) -> str:
        """Property for final SQL field Type.

        It can be changed in subclasses.

        ### Return
        SQL `string`.
        """
        return self._field_type
