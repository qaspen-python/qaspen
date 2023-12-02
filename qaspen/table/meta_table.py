from __future__ import annotations

import copy
import dataclasses
from typing import TYPE_CHECKING, Any, ClassVar, Final

from qaspen.fields.base import Field

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.abc.db_engine import BaseEngine


@dataclasses.dataclass
class MetaTableData:
    """All data about the table.

    ### Arguments:
    - `table_name`: name of the table.

    - `table_schema`: schema where table is located.

    - `abstract`: is it abstract table or not.

    - `table_fields`: all fields of the table.

    - `table_fields_with_default`:
        fields with `default` or `callable_default`.
        We process them before actual application start
        because we don't want to spend time in runtime to
        find fields with defaults.
    - `database_engine`: engine for the database.

    - `alias`: alias for table, usually used in `AS` operator
        and as prefix for the fields.
    """

    table_name: str = ""
    table_schema: str = "public"
    abstract: bool = False
    table_fields: dict[str, Field[Any]] = dataclasses.field(
        default_factory=dict,
    )
    table_fields_with_default: dict[str, Field[Any]] = dataclasses.field(
        default_factory=dict,
    )
    database_engine: BaseEngine[Any, Any, Any] | None = None
    alias: str | None = None


class MetaTable:
    """Meta Table."""

    _table_meta: MetaTableData = MetaTableData()
    _subclasses: ClassVar[list[type[MetaTable]]] = []

    def __init_subclass__(
        cls: type[MetaTable],
        table_name: str | None = None,
        table_schema: str = "public",
        abstract: bool = False,
        **kwargs: Any,
    ) -> None:
        if not table_name:
            table_name = cls.__name__.lower()

        table_fields: Final = cls._parse_table_fields()

        cls._table_meta = MetaTableData(
            table_name=table_name,
            table_schema=table_schema,
            abstract=abstract,
            table_fields=table_fields,
            table_fields_with_default=cls._parse_table_fields_with_default(
                table_fields=table_fields,
            ),
        )

        cls._subclasses.append(cls)

        super().__init_subclass__(**kwargs)

    def __init__(self: Self, **fields_values: Any) -> None:
        """Initialize Table instance.

        This method must be called only from user side.
        """
        for table_field in self._table_meta.table_fields.values():
            new_field = copy.deepcopy(table_field)
            setattr(
                self,
                table_field._original_field_name,
                new_field,
            )

            self._table_meta.table_fields[
                table_field._original_field_name
            ] = new_field

            new_field_value: Any = fields_values.get(
                table_field._original_field_name,
                None,
            )

            setattr(
                self,
                table_field._original_field_name,
                new_field_value,
            )

    def __getattr__(
        self: Self,
        attribute: str,
    ) -> Any:
        return self.__dict__[attribute]  # pragma: no cover

    def __getattribute__(self: Self, attribute: str) -> Any:
        """Return value of the value instead of the instance of the field.

        Value of the field will be returned only
        if we have initialized `instance` of the field,
        not the class.

        ### Params
        :param attribute: the attribute we are accessing.

        ### Returns
        :returns: any attribute of the table.
        """
        table_meta = object.__getattribute__(self, "_table_meta")
        table_fields = object.__getattribute__(table_meta, "table_fields")
        if attribute in table_fields:
            return object.__getattribute__(self, attribute).value
        return super().__getattribute__(attribute)

    @classmethod
    def _retrieve_field(
        cls: type[MetaTable],
        field_name: str,
    ) -> Field[Any]:
        """Retrieve field from the table by its name.

        We need this method because if Field subclass
        uses parameter `db_field_name` than we
        can't get it with basic `BaseTable.your_field`,
        we need to get it from `table_fields`.

        DO NOT USE IT IN YOUR CODE. FOR INTERNAL
        USE ONLY.

        ### Parameters:
        - `field_name`: name of the field.

        ### Returns:
        `Field` in a table.
        """
        return cls._table_meta.table_fields[field_name]

    @classmethod
    def _parse_table_fields(
        cls: type[MetaTable],
    ) -> dict[str, Field[Any]]:
        """Find all `Field` instances.

        ### Returns:
        dict as `dict[<field_name>: <field_instance>]`.
        """
        return {
            field_class._field_data.field_name: field_class
            for field_class in cls.__dict__.values()
            if isinstance(field_class, Field)
        }

    @classmethod
    def _parse_table_fields_with_default(
        cls: type[MetaTable],
        table_fields: dict[str, Field[Any]],
    ) -> dict[str, Field[Any]]:
        """Parse table fields and find all with default values.

        We are searching for non-`None` `default` or `callable_default`
        parameters in `_field_data`.
        """
        return {
            table_field_name: table_field
            for table_field_name, table_field in table_fields.items()
            if (
                table_field._field_data.default
                or table_field._field_data.callable_default
            )
        }

    @classmethod
    def _retrieve_not_abstract_subclasses(
        cls: type[MetaTable],
    ) -> list[type[MetaTable]]:
        """Return all subclasses with `abstract=True`.

        ### Returns:
        `list[type[MetaTable]]`
        """
        return [
            subclass
            for subclass in cls._subclasses
            if not subclass._table_meta.abstract
        ]

    @classmethod
    def _fields_with_default(
        cls: type[MetaTable],
    ) -> dict[str, Field[Any]]:
        return cls._table_meta.table_fields_with_default
