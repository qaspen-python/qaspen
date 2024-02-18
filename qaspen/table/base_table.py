from __future__ import annotations

import copy
import inspect
import typing

from qaspen.columns.base import Column
from qaspen.statements.delete_statement import DeleteStatement
from qaspen.statements.insert_statement import (
    InsertObjectsStatement,
    InsertStatement,
)
from qaspen.statements.select_statement import SelectStatement
from qaspen.statements.update_statement import UpdateStatement
from qaspen.table.meta_table import MetaTable

if typing.TYPE_CHECKING:
    from qaspen.aggregate_functions.base import AggFunction
    from qaspen.qaspen_types import ColumnType

T_ = typing.TypeVar(
    "T_",
    bound="BaseTable",
)


class BaseTable(MetaTable, abstract=True):
    """Main Table in the `Qaspen` library.

    Any Table must be created with this class as base class.
    """

    @classmethod
    def select(
        cls: type[T_],
        *select: Column[typing.Any] | AggFunction,
    ) -> SelectStatement[T_]:
        """Create SelectStatement based on table.

        You can specify here columns from main table and joins,
        aggregate functions.

        :param select_columns: columns to select.
            By default select all possible columns.

        :returns: SelectStatement.

        Example:
        -------
        ```python
        class Buns(BaseTable, table_name="buns"):
            name: VarCharColumn = VarCharColumn()
            nickname: VarCharColumn = VarCharColumn()


        select_statement = Buns.select(
            Buns.name,
        )

        async def main() -> None:
            await insert_statement
        ```
        """
        select_statement: typing.Final[SelectStatement[T_]] = SelectStatement(
            select_objects=select or cls.all_columns(),
            from_table=cls,
        )
        return select_statement

    @classmethod
    def insert(
        cls: type[T_],
        columns: list[Column[typing.Any]],
        values: tuple[list[typing.Any], ...],
    ) -> InsertStatement[T_, None]:
        """Create `InsertStatement`.

        This method copies SQL syntax.

        Parameter `columns` is for columns that
        you specify when you write INSERT query.

        `INSERT INTO qaspen (<there is `columns`>)`.

        Parameter `values` is for actual data that you
        want to insert into the table.

        `!IMPORTANT.`
        You shouldn't specify any value for columns with
        any default value.

        Example:
        -------
        ```python
        class Buns(BaseTable, table_name="buns"):
            name: VarCharColumn = VarCharColumn()
            nickname: VarCharColumn = VarCharColumn()


        columns_to_insert = [
            Buns.name,
            Buns.nickname,
        ]
        values_to_insert = (
            ["Qaspen", "Cool"],
            ["Python", "Awesome"],
            ["Try", "Rust"],
        )
        insert_statement = (
            Buns
            .insert(
                columns=columns_to_insert,
                values=values_to_insert,
            )
        )

        async def main() -> None:
            await insert_statement

        # As a result you will have 3 new rows in the database.
        ```
        """
        return InsertStatement[T_, None](
            from_table=cls,
            columns_to_insert=columns,
            values_to_insert=values,
        )

    @classmethod
    def insert_objects(
        cls: type[T_],
        *insert_objects: T_,
    ) -> InsertObjectsStatement[T_, None]:
        """Create `InsertObjectsStatement`.

        This method allows create new records in database
        based on table objects.

        Example:
        -------
        ```python
        class Buns(BaseTable, table_name="buns"):
            name: VarCharColumn = VarCharColumn()
            nickname: VarCharColumn = VarCharColumn()


        insert_statement = (
            Buns
            .insert_objects(
                Buns(name="Qaspen", nickname="ORM"),
                Buns(name="Python", nickname="Cool"),
            )
        )

        async def main() -> None:
            await insert_statement
        ```
        # As a result you will have 2 new rows in the database.
        """
        return InsertObjectsStatement[T_, None](
            insert_objects=insert_objects,
            from_table=cls,
        )

    @classmethod
    def update(
        cls: type[T_],
        for_update_map: dict[
            Column[typing.Any],
            typing.Any,
        ],
    ) -> UpdateStatement[T_]:
        """Create `UpdateStatement`.

        This method allows update records in the database.

        Example:
        -------
        ```python
        class Buns(BaseTable, table_name="buns"):
            name: VarCharColumn = VarCharColumn()
            nickname: VarCharColumn = VarCharColumn()


        update_statement = (
            Buns
            .update(
                {
                    Buns.name: "New Name",
                    Buns.nickname: "Qaspen",
                }
            ).where(
                Buns.name == "Old name",
            )
        )
        ```
        """
        return UpdateStatement[T_](
            from_table=cls,
            for_update_map=for_update_map,
        )

    @classmethod
    def delete(cls: type[T_]) -> DeleteStatement[T_]:
        """Create `DeleteStatement`.

        This method allows delete records in the database.

        Example:
        -------
        ```python
        class Buns(BaseTable, table_name="buns"):
            name: VarCharColumn = VarCharColumn()
            nickname: VarCharColumn = VarCharColumn()


        update_statement = (
            Buns
            .delete().where(
                Buns.name == "Old name",
            )
        )
        ```
        """
        return DeleteStatement[T_](
            from_table=cls,
        )

    @classmethod
    def all_columns(cls: type[T_]) -> list[Column[ColumnType]]:
        """Return all columns in the table.

        :returns: list of columns.
        """
        return [*cls._table_meta.table_columns.values()]

    @classmethod
    def aliased(cls: type[T_], alias: str) -> type[T_]:
        """Create aliased version of the Table.

        ### Parameters
        :param alias: alias to the column.

        ### Returns
        :returns: Same Table, but aliased.

        Example:
        -------
        ```python
        class Buns(BaseTable, table_name="buns"):
            name: VarCharColumn = VarCharColumn()
            description: VarCharColumn = VarCharColumn()


        # It has the same type with all autosuggestions.
        AliasedBuns = Buns.aliased(alias="PrimaryBuns")
        ```
        """
        return cls._aliased(alias=alias)

    @classmethod
    def _aliased(cls: type[T_], alias: str) -> type[T_]:
        """Add alias to the table.

        We must create new `aliased_table` because in other
        way `alias` change will affect all other queries where
        aliased table is used.

        It'll be used in queries instead of real table name.

        :param alias: string alias to the table.

        :returns: the same table but with the alias.
        """
        aliased_table = type(
            cls.__name__,
            (cls,),
            {},
        )

        attributes = inspect.getmembers(
            cls,
            lambda member: not (inspect.isroutine(member)),
        )
        only_column_attributes: dict[str, Column[typing.Any]] = {
            attribute[0]: copy.deepcopy(attribute[1])
            for attribute in attributes
            if issubclass(type(attribute[1]), Column)
        }

        for table_param_name, table_param in cls.__dict__.items():
            setattr(aliased_table, table_param_name, table_param)

        for column_name, column in only_column_attributes.items():
            column._column_data.from_table = aliased_table
            setattr(aliased_table, column_name, column)

        aliased_table._table_meta = (  # type: ignore[attr-defined]
            copy.deepcopy(
                cls._table_meta,
            )
        )
        aliased_table._table_meta.alias = alias  # type: ignore[attr-defined]

        table_meta_columns = (
            aliased_table._table_meta.table_columns.values()  # type: ignore[attr-defined]
        )
        for column in table_meta_columns:
            column._column_data.from_table = aliased_table

        return aliased_table

    @classmethod
    def is_aliased(cls: type[T_]) -> bool:
        """Return flag that says does the table have an alias.

        :returns: boolean flag.
        """
        return bool(cls._table_meta.alias)

    @classmethod
    def original_table_name(cls: type[T_]) -> str:
        """Return original name of the table.

        Without aliases and other modifications.

        ### Returns:
        Table name as a string.
        """
        return cls._table_meta.table_name

    @classmethod
    def table_name(cls: type[T_]) -> str:
        """Return the original table name or alias to it.

        :returns: original table name or alias.
        """
        return cls._table_meta.alias or cls.original_table_name()

    @classmethod
    def schemed_table_name(cls: type[T_]) -> str:
        """Return the original table name or alias to it with schema.

        :returns: original table name or alias with corresponding schema.
        """
        return f"{cls._table_meta.table_schema}.{cls.table_name()}"

    @classmethod
    def schemed_original_table_name(cls: type[T_]) -> str:
        """Return the original table name with schema.

        :returns: original table name with corresponding schema.
        """
        return f"{cls._table_meta.table_schema}.{cls.original_table_name()}"
