from qaspen.fields.fields import TextField, VarCharField
from qaspen.querystring.querystring import QueryString
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


print(
    User.select(
        User.all_fields(),
    ).where(
        User.select([User.name]).where(User.name > "Sasha").exists(),
    ).build_query()
)


print(
    User.select(
        User.all_fields(),
    ).where(
        User.name > "123",
    ).exists().make_sql_string(),
)
