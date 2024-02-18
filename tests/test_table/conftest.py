from qaspen.columns.primitive import VarCharColumn
from qaspen.table.base_table import BaseTable
from qaspen.table.meta_table import MetaTable


class InheritanceMetaTable(MetaTable):
    """Table for tests."""

    column1: VarCharColumn = VarCharColumn(is_null=True)
    column2: VarCharColumn = VarCharColumn(is_null=True)


class InheritanceBetaTable(BaseTable, table_name="btable"):
    """Table for tests."""

    column1: VarCharColumn = VarCharColumn(is_null=True)
    column2: VarCharColumn = VarCharColumn(is_null=True)
