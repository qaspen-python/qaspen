from __future__ import annotations

from typing import Final

from qaspen.aggregate_functions.general_purpose import Count
from qaspen.columns.primitive import VarCharColumn
from qaspen.table.base_table import BaseTable
from tests.test_table.conftest import InheritanceBetaTable


def test_base_table_select() -> None:
    """Test `select` method."""
    select_stmt_with_columns = InheritanceBetaTable.select(
        InheritanceBetaTable.column1,
        InheritanceBetaTable.column2,
    )

    querystring, qs_params = select_stmt_with_columns.querystring().build()
    assert (
        querystring
        == "SELECT btable.column1, btable.column2 FROM public.btable"
    )
    assert not qs_params

    select_stmt_with_agg = InheritanceBetaTable.select(
        Count(InheritanceBetaTable.column1),
    )

    querystring, qs_params = select_stmt_with_agg.querystring().build()
    assert querystring == "SELECT COUNT(btable.column1) FROM public.btable"
    assert not qs_params

    select_stmt_with_columns_and_agg = InheritanceBetaTable.select(
        Count(InheritanceBetaTable.column1),
        InheritanceBetaTable.column2,
    )

    (
        querystring,
        qs_params,
    ) = select_stmt_with_columns_and_agg.querystring().build()
    assert (
        querystring
        == "SELECT btable.column2, COUNT(btable.column1) FROM public.btable"
    )
    assert not qs_params


def test_base_table_insert() -> None:
    """Test `insert` method."""
    insert_stmt = InheritanceBetaTable.insert(
        columns=[
            InheritanceBetaTable.column1,
            InheritanceBetaTable.column2,
        ],
        values=(
            ["Qaspen", "Cool"],
            ["Python", "Nice"],
        ),
    )

    querystring, qs_params = insert_stmt.querystring().build()
    assert (
        querystring
        == "INSERT INTO btable(column1, column2) VALUES (%s, %s), (%s, %s) "
    )
    assert qs_params == ["Qaspen", "Cool", "Python", "Nice"]


def test_base_table_insert_objects() -> None:
    """Test `insert_objects` method."""
    insert_stmt = InheritanceBetaTable.insert_objects(
        InheritanceBetaTable(
            column1="test",
            column2="Wow",
        ),
    )

    assert (
        insert_stmt.querystring().build()[0]
        == "INSERT INTO btable(column1, column2) VALUES (%s, %s)"
    )


def test_base_table_all_columns() -> None:
    """Test `all_columns` method."""

    class FotTestInheritanceBetaTable(BaseTable):
        column1: VarCharColumn = VarCharColumn()
        column2: VarCharColumn = VarCharColumn()

    assert FotTestInheritanceBetaTable.all_columns() == [
        FotTestInheritanceBetaTable.column1,
        FotTestInheritanceBetaTable.column2,
    ]


def test_base_table_update_method() -> None:
    """Test `update()` method."""
    update_stmt = (
        InheritanceBetaTable.update(
            for_update_map={
                InheritanceBetaTable.column1: "TestNew",
            },
        )
        .where(
            InheritanceBetaTable.column2 == "NotNew",
        )
        .returning(
            InheritanceBetaTable.column1,
        )
    )

    querystring, qs_params = update_stmt.querystring().build()

    assert (
        querystring
        == "UPDATE btable SET column1 = %s WHERE btable.column2 = %s RETURNING btable.column1"  # noqa: E501
    )
    assert qs_params == ["TestNew", "NotNew"]


def test_base_table_delete_method() -> None:
    """Test `delete()` method."""
    update_stmt = (
        InheritanceBetaTable.delete()
        .where(
            InheritanceBetaTable.column2 == "NotNew",
        )
        .returning(
            InheritanceBetaTable.column1,
        )
    )

    querystring, qs_params = update_stmt.querystring().build()

    assert (
        querystring
        == "DELETE FROM btable WHERE btable.column2 = %s RETURNING btable.column1"  # noqa: E501
    )
    assert qs_params == ["NotNew"]


def test_base_table_aliased_method() -> None:
    """Test `aliased` method."""
    table_alias = "column_wow"
    aliased = InheritanceBetaTable.aliased(
        alias=table_alias,
    )

    assert aliased.is_aliased()
    assert aliased._table_meta.alias == table_alias


def test_base_table_original_table_name_method() -> None:
    """Test `original_table_name` method."""
    assert InheritanceBetaTable.original_table_name() == "btable"

    aliased = InheritanceBetaTable.aliased(
        alias="column_wow",
    )

    assert aliased.original_table_name() == "btable"


def test_base_table_table_name_method() -> None:
    """Test `table_name` method."""
    assert InheritanceBetaTable.table_name() == "btable"

    aliased = InheritanceBetaTable.aliased(
        alias="column_wow",
    )

    assert aliased.table_name() == "column_wow"


def test_base_table_schemed_table_name_method() -> None:
    """Test `schemed_table_name` method."""
    schema: Final = "not_public"
    table_name: Final = "not_name"
    table_alias = "column_wow"

    class FotTestInheritanceBetaTable(
        BaseTable,
        table_name=table_name,
        table_schema=schema,
    ):
        column1: VarCharColumn = VarCharColumn()
        column2: VarCharColumn = VarCharColumn()

    assert (
        FotTestInheritanceBetaTable.schemed_table_name()
        == f"not_public.{table_name}"
    )

    aliased = FotTestInheritanceBetaTable.aliased(
        alias=table_alias,
    )
    assert aliased.schemed_table_name() == f"not_public.{table_alias}"


def test_base_table_schemed_original_table_name_method() -> None:
    """Test `schemed_original_table_name` method."""
    schema: Final = "not_public"
    table_name: Final = "not_name"
    table_alias = "column_wow"

    class FotTestInheritanceBetaTable(
        BaseTable,
        table_name=table_name,
        table_schema=schema,
    ):
        column1: VarCharColumn = VarCharColumn()
        column2: VarCharColumn = VarCharColumn()

    assert (
        FotTestInheritanceBetaTable.schemed_original_table_name()
        == f"not_public.{table_name}"
    )

    aliased = FotTestInheritanceBetaTable.aliased(
        alias=table_alias,
    )
    assert aliased.schemed_original_table_name() == f"not_public.{table_name}"
