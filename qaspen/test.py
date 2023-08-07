from qaspen.fields.string_fields.fields import TextField, VarCharField
from qaspen.table.base_table import BaseTable


class User(BaseTable, table_name="users"):
    name: VarCharField = VarCharField(default="Sasha")
    surname: VarCharField = VarCharField(default="Kiselev")
    description: TextField = TextField(default="Zopa")


# print(User.select(User.all_fields()).build_query())
print(User.select(select_fields=[User.name, User.surname]).build_query())
print(User.update(
    to_update_fields={
        User.name: "Sasha",
        User.surname: "Kiselev",
        User.description: "Loshara",
    },
).build_query())


# @dataclasses.dataclass
# class Test:
#     a: int = 1


# u = User()
# print(u.name)
# # print(u)