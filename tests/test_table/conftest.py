from qaspen.fields.primitive import VarCharField
from qaspen.table.base_table import BaseTable
from qaspen.table.meta_table import MetaTable


class InheritanceMetaTable(MetaTable):
    """Table for tests."""

    field1: VarCharField = VarCharField(is_null=True)
    field2: VarCharField = VarCharField(is_null=True)


class InheritanceBetaTable(BaseTable, table_name="btable"):
    """Table for tests."""

    field1: VarCharField = VarCharField(is_null=True)
    field2: VarCharField = VarCharField(is_null=True)
