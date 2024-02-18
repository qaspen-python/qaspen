from __future__ import annotations

from typing import Final

from qaspen.columns.primitive import VarCharColumn
from qaspen.table.meta_table import MetaTable
from tests.test_table.conftest import InheritanceMetaTable


def test_meta_table_init_subclass() -> None:
    """Test `__init__subclass__` method."""
    table_name: Final = "test_name"
    table_schema: Final = "non_public"

    def column4_default() -> str:
        return "wow!"

    class InheritanceMetaTable(
        MetaTable,
        table_name=table_name,
        table_schema=table_schema,
        abstract=True,
    ):
        column1: VarCharColumn = VarCharColumn()
        column2: VarCharColumn = VarCharColumn(db_column_name="non_column2")
        column3: VarCharColumn = VarCharColumn(
            default="123",
        )
        column4: VarCharColumn = VarCharColumn(
            default=column4_default,
        )

    assert InheritanceMetaTable._table_meta.table_name == table_name
    assert InheritanceMetaTable._table_meta.table_schema == table_schema
    assert InheritanceMetaTable._table_meta.abstract
    assert InheritanceMetaTable in InheritanceMetaTable._subclasses
    assert InheritanceMetaTable._table_meta.table_columns == {
        "column1": InheritanceMetaTable.column1,  # type: ignore[arg-type]
        "non_column2": InheritanceMetaTable.column2,  # type: ignore[arg-type]
        "column3": InheritanceMetaTable.column3,  # type: ignore[arg-type]
        "column4": InheritanceMetaTable.column4,  # type: ignore[arg-type]
    }
    assert InheritanceMetaTable._table_meta.table_columns_with_default == {
        "column3": InheritanceMetaTable.column3,  # type: ignore[arg-type]
        "column4": InheritanceMetaTable.column4,  # type: ignore[arg-type]
    }
    assert InheritanceMetaTable._columns_with_default() == {
        "column3": InheritanceMetaTable.column3,  # type: ignore[arg-type]
        "column4": InheritanceMetaTable.column4,  # type: ignore[arg-type]
    }
    assert InheritanceMetaTable._columns_with_default() == {
        "column3": InheritanceMetaTable.column3,  # type: ignore[arg-type]
        "column4": "wow",
    }


def test_meta_table_init_method() -> None:
    """Test `__init__` method."""
    inited_table = InheritanceMetaTable(
        column1="test",
    )

    assert inited_table.column1 == "test"  # type: ignore[arg-type]
    assert inited_table.column2 is None  # type: ignore[arg-type]


def test_meta_table_getattribute_method() -> None:
    """Test `__getattribute__` method."""
    assert isinstance(
        InheritanceMetaTable.column1,  # type: ignore[arg-type]
        VarCharColumn,
    )
    inited_table = InheritanceMetaTable(
        column1="test",
    )
    assert inited_table.column1 == "test"  # type: ignore[arg-type]


def test_meta_table_retrieve_not_abstract_subclasses() -> None:
    """Test `_retrieve_not_abstract_subclasses` method."""
    assert (
        # This table from conftest
        InheritanceMetaTable
        in MetaTable._retrieve_not_abstract_subclasses()
    )
