from qaspen.fields.fields import TextField, VarCharField
from qaspen.table.base_table import BaseTable


class User(BaseTable, table_name="users"):
    name: VarCharField = VarCharField(default="Sasha")
    surname: VarCharField = VarCharField(default="Kiselev")
    description: TextField = TextField(default="Zopa")


print(VarCharField(default="Sasha") > "123")

print(User.name > "123")


print(
    User.select(
        User.all_fields(),
    ).where(
        User.name > "Sasha",
    )
)
# print(User.select(select_fields=[User.name, User.surname]).build_query())
# print(User.update(
#     update_fields={
#         User.name: "Sasha",
#         User.surname: "Kiselev",
#         User.description: "Loshara",
#     },
# ).build_query())




# @dataclasses.dataclass
# class Test:
#     a: int = 1


# u = User()
# print(u.name)
# # print(u)