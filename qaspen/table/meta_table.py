from __future__ import annotations

import copy
import dataclasses
from typing import TYPE_CHECKING, Any, ClassVar, Final

from qaspen.columns.base import Column

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

    - `table_columns`: all columns of the table.

    - `table_columns_with_default`:
        columns with `default` or `callable_default`.
        We process them before actual application start
        because we don't want to spend time in runtime to
        find columns with defaults.

    - `database_engine`: engine for the database.

    - `alias`: alias for table, usually used in `AS` operator
        and as prefix for the columns.
    """

    table_name: str = ""
    table_schema: str = "public"
    abstract: bool = False
    table_columns: dict[str, Column[Any]] = dataclasses.field(
        default_factory=dict,
    )
    table_columns_with_default: dict[str, Column[Any]] = dataclasses.field(
        default_factory=dict,
    )
    database_engine: BaseEngine[Any, Any, Any] | None = None
    alias: str | None = None

    def __deepcopy__(self: Self, memo: Any) -> MetaTableData:
        return MetaTableData(
            table_name=copy.copy(self.table_name),
            table_schema=copy.copy(self.table_schema),
            abstract=copy.copy(self.abstract),
            table_columns=copy.deepcopy(self.table_columns),
            table_columns_with_default=copy.deepcopy(
                self.table_columns_with_default,
            ),
            database_engine=self.database_engine,
            alias=copy.copy(self.alias),
        )


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

        table_columns: Final = cls._parse_table_columns()

        cls._table_meta = MetaTableData(
            table_name=table_name,
            table_schema=table_schema,
            abstract=abstract,
            table_columns=table_columns,
            table_columns_with_default=cls._parse_table_columns_with_default(
                table_columns=table_columns,
            ),
        )

        cls._subclasses.append(cls)

        super().__init_subclass__(**kwargs)

    def __init__(self: Self, **columns_values: Any) -> None:
        """Initialize Table instance.

        This method must be called only from user side.
        """
        self._table_meta = copy.deepcopy(self._table_meta)
        for table_column in self._table_meta.table_columns.values():
            self._table_meta.table_columns[
                table_column._original_column_name
            ] = table_column
            setattr(
                self,
                table_column._original_column_name,
                table_column,
            )

            new_column_value: Any = columns_values.get(
                table_column._original_column_name,
                None,
            )

            setattr(
                self,
                table_column._original_column_name,
                new_column_value,
            )

    def __getattr__(
        self: Self,
        attribute: str,
    ) -> Any:
        return self.__dict__[attribute]  # pragma: no cover

    def __getattribute__(self: Self, attribute: str) -> Any:
        """Return value of the value instead of the instance of the column.

        Value of the column will be returned only
        if we have initialized `instance` of the column,
        not the class.

        ### Params
        :param attribute: the attribute we are accessing.

        ### Returns
        :returns: any attribute of the table.
        """
        table_meta = object.__getattribute__(self, "_table_meta")
        table_columns = object.__getattribute__(table_meta, "table_columns")
        if attribute in table_columns:
            return object.__getattribute__(self, attribute).value
        return super().__getattribute__(attribute)

    @classmethod
    def _retrieve_column(
        cls: type[MetaTable],
        column_name: str,
    ) -> Column[Any]:
        """Retrieve column from the table by its name.

        We need this method because if Column subclass
        uses parameter `db_column_name` than we
        can't get it with basic `BaseTable.your_column`,
        we need to get it from `table_columns`.

        DO NOT USE IT IN YOUR CODE. FOR INTERNAL
        USE ONLY.

        ### Parameters:
        - `column_name`: name of the column.

        ### Returns:
        `Column` in a table.
        """
        return cls._table_meta.table_columns[column_name]

    @classmethod
    def _parse_table_columns(
        cls: type[MetaTable],
    ) -> dict[str, Column[Any]]:
        """Find all `Column` instances.

        ### Returns:
        dict as `dict[<column_name>: <column_instance>]`.
        """
        return {
            column_class._column_data.column_name: column_class
            for column_class in cls.__dict__.values()
            if isinstance(column_class, Column)
        }

    @classmethod
    def _parse_table_columns_with_default(
        cls: type[MetaTable],
        table_columns: dict[str, Column[Any]],
    ) -> dict[str, Column[Any]]:
        """Parse table columns and find all with default values.

        We are searching for non-`None` `default` or `callable_default`
        parameters in `_column_data`.
        """
        return {
            table_column_name: table_column
            for table_column_name, table_column in table_columns.items()
            if (
                table_column._column_data.default
                or table_column._column_data.callable_default
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
    def _columns_with_default(
        cls: type[MetaTable],
    ) -> dict[str, Column[Any]]:
        return cls._table_meta.table_columns_with_default
