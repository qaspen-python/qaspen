import abc
import copy
import dataclasses
import typing

if typing.TYPE_CHECKING:
    from qaspen.table.base_table import BaseTable


FieldType = typing.TypeVar(
    "FieldType",
)

FieldDefaultType = typing.Union[
    FieldType,
    typing.Callable[
        [],
        typing.Union[FieldType, typing.Any],
    ],
    None,
]


class EmptyFieldValue:
    """Indicates that field wasn't queried from the database."""

    def __str__(self: typing.Self) -> str:
        return self.__class__.__name__


@dataclasses.dataclass
class FieldData(typing.Generic[FieldType]):
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
    from_table: type["BaseTable"] = None  # type: ignore[assignment]
    is_null: bool = False
    field_value: FieldType | EmptyFieldValue | None = EmptyFieldValue()
    default: FieldDefaultType[FieldType] = None
    prefix: str = ""
    alias: str = ""
    in_join: bool = False


class BaseField(abc.ABC, typing.Generic[FieldType]):
    """Base field class for all Fields."""

    _field_data: FieldData[FieldType]

    def __set_name__(
        self: typing.Self,
        owner: typing.Any,
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

    def _is_the_same_field(
        self: typing.Self,
        second_field: "BaseField[FieldType]",
    ) -> bool:
        """Compare two fields.

        Return `True` if they are the same, else `False`.
        They are equal if they `_field_data`s are the same.
        """
        return self._field_data == second_field._field_data

    @abc.abstractmethod
    def _make_field_create_statement(
        self: typing.Self,
    ) -> str:
        ...

    @abc.abstractmethod
    def _validate_field_value(
        self: typing.Self,
        field_value: FieldType | None,
    ) -> None:
        """Validate field value.

        It must raise an error if something goes wrong
        or not return anything.
        """
        ...

    @abc.abstractmethod
    def _validate_default_value(
        self: typing.Self,
        default_value: FieldType | None,
    ) -> None:
        """Validate field default value.

        It must raise an error in validation failed.
        """
        ...

    @property
    def value(self: typing.Self) -> FieldType | EmptyFieldValue | None:
        return self._field_data.field_value

    @property
    def table_name(self: typing.Self) -> str:
        """Return the table name of this field."""
        return self._field_data.from_table.original_table_name()

    def _with_prefix(self: typing.Self, prefix: str) -> "BaseField[FieldType]":
        field: BaseField[FieldType] = copy.deepcopy(self)
        field._field_data.prefix = prefix
        return field

    def _with_alias(self: typing.Self, alias: str) -> "BaseField[FieldType]":
        field: BaseField[FieldType] = copy.deepcopy(self)
        field._field_data.alias = alias
        return field

    @property
    def original_field_name(self: typing.Self) -> str:
        """Return name of the field without prefix and alias."""
        return self._field_data.field_name

    @property
    def field_name(self: typing.Self) -> str:
        """Return field name with prefix and alias."""
        prefix: str = (
            self._field_data.from_table._table_meta.alias
            or self._field_data.prefix
            or self._field_data.from_table.table_name()
        )
        field_name: str = f"{prefix}.{self._field_data.field_name}"
        if prefix := self._field_data.alias:
            field_name += f" AS {prefix}"

        return field_name

    @property
    def default(self: typing.Self) -> FieldDefaultType[FieldType]:
        """Return default value of the field."""
        return self._field_data.default

    @property
    def is_null(self: typing.Self) -> bool:
        """Return flag that field can be `NULL`."""
        return self._field_data.is_null

    @property
    def _field_null(self: typing.Self) -> str:
        return "NOT NULL" if not self.is_null else ""

    @property
    def _field_default(self: typing.Self) -> str:
        if self.default and not callable(self.default):
            return f"DEFAULT {self.default}" if self.default else ""
        return ""

    @property
    def _default_field_type(self: typing.Self) -> str:
        return self.__class__.__name__.upper()

    @property
    def _sql_type(self: typing.Self) -> str:
        return self._default_field_type
