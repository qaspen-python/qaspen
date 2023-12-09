from __future__ import annotations

from typing import Final

from qaspen.aggregate_functions.general_purpose import Count
from qaspen.fields.primitive import VarCharField
from qaspen.table.base_table import BaseTable
from tests.test_table.conftest import InheritanceBetaTable


def test_base_table_select() -> None:
    """Test `select` method."""
    select_stmt_with_fields = InheritanceBetaTable.select(
        InheritanceBetaTable.field1,
        InheritanceBetaTable.field2,
    )

    querystring, qs_params = select_stmt_with_fields.querystring().build()
    assert (
        querystring == "SELECT btable.field1, btable.field2 FROM public.btable"
    )
    assert not qs_params

    select_stmt_with_agg = InheritanceBetaTable.select(
        Count(InheritanceBetaTable.field1),
    )

    querystring, qs_params = select_stmt_with_agg.querystring().build()
    assert querystring == "SELECT COUNT(btable.field1) FROM public.btable"
    assert not qs_params

    select_stmt_with_fields_and_agg = InheritanceBetaTable.select(
        Count(InheritanceBetaTable.field1),
        InheritanceBetaTable.field2,
    )

    (
        querystring,
        qs_params,
    ) = select_stmt_with_fields_and_agg.querystring().build()
    assert (
        querystring
        == "SELECT btable.field2, COUNT(btable.field1) FROM public.btable"
    )
    assert not qs_params


def test_base_table_insert() -> None:
    """Test `insert` method."""
    insert_stmt = InheritanceBetaTable.insert(
        fields=[
            InheritanceBetaTable.field1,
            InheritanceBetaTable.field2,
        ],
        values=(
            ["Qaspen", "Cool"],
            ["Python", "Nice"],
        ),
    )

    querystring, qs_params = insert_stmt.querystring().build()
    assert (
        querystring
        == "INSERT INTO btable(field1, field2) VALUES (%s, %s), (%s, %s) "
    )
    assert qs_params == ["Qaspen", "Cool", "Python", "Nice"]


def test_base_table_insert_objects() -> None:
    """Test `insert_objects` method."""
    insert_stmt = InheritanceBetaTable.insert_objects(
        InheritanceBetaTable(
            field1="test",
            field2="Wow",
        ),
    )

    assert (
        insert_stmt.querystring().build()[0]
        == "INSERT INTO btable(field1, field2) VALUES (%s, %s)"
    )


def test_base_table_all_fields() -> None:
    """Test `all_fields` method."""

    class FotTestInheritanceBetaTable(BaseTable):
        field1: VarCharField = VarCharField()
        field2: VarCharField = VarCharField()

    assert FotTestInheritanceBetaTable.all_fields() == [
        FotTestInheritanceBetaTable.field1,
        FotTestInheritanceBetaTable.field2,
    ]


def test_base_table_aliased_method() -> None:
    """Test `aliased` method."""
    table_alias = "field_wow"
    aliased = InheritanceBetaTable.aliased(
        alias=table_alias,
    )

    assert aliased.is_aliased()
    assert aliased._table_meta.alias == table_alias


def test_base_table_original_table_name_method() -> None:
    """Test `original_table_name` method."""
    assert InheritanceBetaTable.original_table_name() == "btable"

    aliased = InheritanceBetaTable.aliased(
        alias="field_wow",
    )

    assert aliased.original_table_name() == "btable"


def test_base_table_table_name_method() -> None:
    """Test `table_name` method."""
    assert InheritanceBetaTable.table_name() == "btable"

    aliased = InheritanceBetaTable.aliased(
        alias="field_wow",
    )

    assert aliased.table_name() == "field_wow"


def test_base_table_schemed_table_name_method() -> None:
    """Test `schemed_table_name` method."""
    schema: Final = "not_public"
    table_name: Final = "not_name"
    table_alias = "field_wow"

    class FotTestInheritanceBetaTable(
        BaseTable,
        table_name=table_name,
        table_schema=schema,
    ):
        field1: VarCharField = VarCharField()
        field2: VarCharField = VarCharField()

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
    table_alias = "field_wow"

    class FotTestInheritanceBetaTable(
        BaseTable,
        table_name=table_name,
        table_schema=schema,
    ):
        field1: VarCharField = VarCharField()
        field2: VarCharField = VarCharField()

    assert (
        FotTestInheritanceBetaTable.schemed_original_table_name()
        == f"not_public.{table_name}"
    )

    aliased = FotTestInheritanceBetaTable.aliased(
        alias=table_alias,
    )
    assert aliased.schemed_original_table_name() == f"not_public.{table_name}"
