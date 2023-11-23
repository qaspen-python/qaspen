from qaspen.fields.primitive import VarCharField
from qaspen.table.meta_table import MetaTable


class InheritanceMetaTable(MetaTable):
    """Table for tests."""

    field1: VarCharField = VarCharField()
    field2: VarCharField = VarCharField()
