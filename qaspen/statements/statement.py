import dataclasses

from qaspen.table.meta_table import MetaTable


@dataclasses.dataclass
class Statement:
    """Dataclass for all SQL queries."""
    from_table: type[MetaTable]
