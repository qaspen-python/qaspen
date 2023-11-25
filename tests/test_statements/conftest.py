from qaspen.fields.primitive import TextField
from qaspen.table.base_table import BaseTable


class ForTestTable(BaseTable):
    """Class for test purposes."""

    name: TextField = TextField()
    count: TextField = TextField()
