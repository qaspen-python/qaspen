from __future__ import annotations

from qaspen.fields.primitive import IntegerField, VarCharField
from qaspen.table.base_table import BaseTable


class TableForTest(BaseTable, table_name="ttest"):
    """Table for test proposes."""

    name: VarCharField = VarCharField()
    count: IntegerField = IntegerField()
