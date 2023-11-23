from __future__ import annotations

import copy
import dataclasses
from typing import TYPE_CHECKING, Any, ClassVar, Final

from qaspen.fields.base import Field
from qaspen.qaspen_types import EMPTY_FIELD_VALUE

if TYPE_CHECKING:  # pragma: no cover
    from typing_extensions import Self

    from qaspen.abc.db_engine import BaseEngine


@dataclasses.dataclass
class MetaTableData:
    """All data about the table."""

    table_name: str = ""
    table_schema: str = "public"
    abstract: bool = False
    table_fields: dict[str, Field[Any]] = dataclasses.field(
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

        table_fields: Final[dict[str, Field[Any]],] = cls._parse_table_fields()

        cls._table_meta = MetaTableData(
            table_name=table_name,
            table_schema=table_schema,
            abstract=abstract,
            table_fields=table_fields,
        )

        cls._subclasses.append(cls)

        super().__init_subclass__(**kwargs)

    def __init__(self: Self, **fields_values: Any) -> None:
        for table_field in self._table_meta.table_fields.values():
            setattr(
                self,
                table_field._original_field_name,
                copy.deepcopy(table_field),
            )
            new_field_value: Any = fields_values.get(
                table_field._original_field_name,
                EMPTY_FIELD_VALUE,
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
        return self.__dict__[attribute]

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
