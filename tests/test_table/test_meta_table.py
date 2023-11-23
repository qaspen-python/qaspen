from __future__ import annotations

from typing import Final

from qaspen.fields.primitive import VarCharField
from qaspen.qaspen_types import EMPTY_FIELD_VALUE
from qaspen.table.meta_table import MetaTable
from tests.test_table.conftest import InheritanceMetaTable


def test_meta_table_init_subclass() -> None:
    """Test `__init__subclass__` method."""
    table_name: Final = "test_name"
    table_schema: Final = "non_public"

    class InheritanceMetaTable(
        MetaTable,
        table_name=table_name,
        table_schema=table_schema,
        abstract=True,
    ):
        pass

    assert InheritanceMetaTable._table_meta.table_name == table_name
    assert InheritanceMetaTable._table_meta.table_schema == table_schema
    assert InheritanceMetaTable._table_meta.abstract


def test_meta_table_init_method() -> None:
    """Test `__init__` method."""
    inited_table = InheritanceMetaTable(
        field1="test",
    )

    assert inited_table.field1 == "test"  # type: ignore[arg-type]
    assert inited_table.field2 == EMPTY_FIELD_VALUE  # type: ignore[arg-type]


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
