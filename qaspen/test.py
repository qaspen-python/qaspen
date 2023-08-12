from qaspen.fields.fields import TextField, VarCharField
from qaspen.statements.combinable_statements.order_by_statement import OrderBy
from qaspen.statements.combinable_statements.where_statement import (
    WhereExclusive,
)
from qaspen.statements.select_statement import SelectStatement
from qaspen.table.base_table import BaseTable


class User(BaseTable, table_name="users"):
    name: VarCharField = VarCharField(default="Sasha")
    surname: VarCharField = VarCharField(default="Kiselev")
    description: TextField = TextField(default="Zopa")


stat1 = User.select(
    User.all_fields(),
).where(
    User.name == "WOW",
)

print(stat1.build_query())

stat2 = (
    User
    .select(
        [
            User.name,
            User.surname,
            User.description,
        ]
    )
)

print(stat1.union(stat2).build_query())
