from qaspen.fields.fields import TextField, VarCharField
from qaspen.statements.combinable_statements.order_by_statement import OrderBy
from qaspen.statements.combinable_statements.where_statement import WhereExclusive
from qaspen.statements.select_statement import SelectStatement
from qaspen.table.base_table import BaseTable


class User(BaseTable, table_name="users"):
    name: VarCharField = VarCharField(default="Sasha")
    surname: VarCharField = VarCharField(default="Kiselev")
    description: TextField = TextField(default="Zopa")


a = (
    User.select([User.name])
    .where(
        User.name == "a"
    )
)

b = (
    User.select([User.surname])
    .where(
        User.name == "b"
    )
)


print(id(a))
print(id(b))

print(a.build_query())