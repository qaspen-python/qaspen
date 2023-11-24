from __future__ import annotations

from qaspen.aggregate_functions.general_purpose import Count
from tests.test_table.conftest import InheritanceBetaTable


def test_base_table_select() -> None:
    """Test `select` method."""
    select_stmt_with_fields = InheritanceBetaTable.select(
        InheritanceBetaTable.field1,
        InheritanceBetaTable.field2,
    )

    assert (
        str(select_stmt_with_fields.querystring())
        == "SELECT btable.field1, btable.field2 FROM public.btable"
    )

    select_stmt_with_agg = InheritanceBetaTable.select(
        Count(InheritanceBetaTable.field1),
    )

    assert (
        str(select_stmt_with_agg.querystring())
        == "SELECT COUNT(btable.field1) FROM public.btable"
    )

    select_stmt_with_fields_and_agg = InheritanceBetaTable.select(
        Count(InheritanceBetaTable.field1),
        InheritanceBetaTable.field2,
    )

    assert (
        str(select_stmt_with_fields_and_agg.querystring())
        == "SELECT btable.field2, COUNT(btable.field1) FROM public.btable"
    )
