import dataclasses
from qaspen.statements.statement import Statement
from qaspen.table.meta_table import MetaTable


@dataclasses.dataclass
class InsertStatement(Statement):
    insert_records: tuple[MetaTable, ...]
