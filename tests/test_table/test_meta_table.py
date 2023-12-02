from __future__ import annotations

from typing import Final

from qaspen.fields.primitive import VarCharField
from qaspen.table.meta_table import MetaTable
from tests.test_table.conftest import InheritanceMetaTable


def test_meta_table_init_subclass() -> None:
    """Test `__init__subclass__` method."""
    table_name: Final = "test_name"
    table_schema: Final = "non_public"

    def field4_default() -> str:
        return "wow!"

    class InheritanceMetaTable(
        MetaTable,
        table_name=table_name,
        table_schema=table_schema,
        abstract=True,
    ):
        field1: VarCharField = VarCharField()
        field2: VarCharField = VarCharField(db_field_name="non_field2")
        field3: VarCharField = VarCharField(
            default="123",
        )
        field4: VarCharField = VarCharField(
            default=field4_default,
        )

    assert InheritanceMetaTable._table_meta.table_name == table_name
    assert InheritanceMetaTable._table_meta.table_schema == table_schema
    assert InheritanceMetaTable._table_meta.abstract
    assert InheritanceMetaTable in InheritanceMetaTable._subclasses
    assert InheritanceMetaTable._table_meta.table_fields == {
        "field1": InheritanceMetaTable.field1,  # type: ignore[arg-type]
        "non_field2": InheritanceMetaTable.field2,  # type: ignore[arg-type]
        "field3": InheritanceMetaTable.field3,  # type: ignore[arg-type]
        "field4": InheritanceMetaTable.field4,  # type: ignore[arg-type]
    }
    assert InheritanceMetaTable._table_meta.table_fields_with_default == {
        "field3": InheritanceMetaTable.field3,  # type: ignore[arg-type]
        "field4": InheritanceMetaTable.field4,  # type: ignore[arg-type]
    }
    assert InheritanceMetaTable._fields_with_default() == {
        "field3": InheritanceMetaTable.field3,  # type: ignore[arg-type]
        "field4": InheritanceMetaTable.field4,  # type: ignore[arg-type]
    }


def test_meta_table_init_method() -> None:
    """Test `__init__` method."""
    inited_table = InheritanceMetaTable(
        field1="test",
    )

    assert inited_table.field1 == "test"  # type: ignore[arg-type]
    assert inited_table.field2 is None  # type: ignore[arg-type]


def test_meta_table_getattribute_method() -> None:
    """Test `__getattribute__` method."""
    assert isinstance(
        InheritanceMetaTable.field1,  # type: ignore[arg-type]
        VarCharField,
    )
    inited_table = InheritanceMetaTable(
        field1="test",
    )
    assert inited_table.field1 == "test"  # type: ignore[arg-type]


def test_meta_table_retrieve_not_abstract_subclasses() -> None:
    """Test `_retrieve_not_abstract_subclasses` method."""
    assert (
        # This table from conftest
        InheritanceMetaTable
        in MetaTable._retrieve_not_abstract_subclasses()
    )
