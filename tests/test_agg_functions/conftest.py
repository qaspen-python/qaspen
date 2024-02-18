from __future__ import annotations

from qaspen.columns.primitive import IntegerColumn, VarCharColumn
from qaspen.table.base_table import BaseTable


class TableForTest(BaseTable, table_name="ttest"):
    """Table for test proposes."""

    name: VarCharColumn = VarCharColumn()
    count: IntegerColumn = IntegerColumn()
