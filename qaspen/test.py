from qaspen.fields.fields import TextField, VarCharField
from qaspen.statements.sub_statements.order_by_statement import OrderBy
from qaspen.table.base_table import BaseTable


class User(BaseTable, table_name="users"):
    name: VarCharField = VarCharField(default="Sasha")
    surname: VarCharField = VarCharField(default="Kiselev")
    description: TextField = TextField(default="Zopa")


print(
    User.select(
        User.all_fields(),
    ).where(
        (User.name.not_contains(comparison_values=["123", "555", "123"]))
        & (User.surname == "NO")
        & User.name.between(
            left_value="123",
            right_value="55",
        )
        & User.name.not_ilike("123")
        & User.name.ilike("123")
    )
    .order_by(
        User.name,
        ascending=False,
        nulls_first=False,
        order_by_statements=(
            OrderBy(
                field=User.surname,
            ),
            OrderBy(
                field=User.description,
                ascending=False,
                nulls_first=False,
            ),
        )
    )
    .limit(123)
    .build_query()
)

# print(
#     User.select(
#         User.all_fields(),
#     ).where(
#         (User.name > "123") | (User.surname > "Kis"),
#         # (User.name == "Sasha"),
#     ).where(
#         User.surname != "AB",
#     ).build_query()
# )
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