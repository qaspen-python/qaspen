from qaspen.fields.primitive import VarCharField
from qaspen.table.base_table import BaseTable


class ForTestTable(BaseTable):
    """Class for test purposes."""

    name: VarCharField = VarCharField()
    count: VarCharField = VarCharField()
