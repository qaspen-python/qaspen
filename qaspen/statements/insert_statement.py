import dataclasses
from qaspen.table.meta_table import MetaTable


@dataclasses.dataclass
class InsertStatement:
    insert_records: tuple[MetaTable, ...]
