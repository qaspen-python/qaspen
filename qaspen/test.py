from qaspen.fields.fields import TextField, VarCharField
from qaspen.statements.combinable_statements.order_by_statement import OrderBy
from qaspen.statements.combinable_statements.where_statement import WhereExclusive
from qaspen.table.base_table import BaseTable


class User(BaseTable, table_name="users"):
    name: VarCharField = VarCharField(default="Sasha")
    surname: VarCharField = VarCharField(default="Kiselev")
    description: TextField = TextField(default="Zopa")


statement = (
    User
    .select(User.all_fields())
    .where(
        WhereExclusive(
            WhereExclusive(
                User.name.eq("Sasha") | User.name.like("123")
            ) & WhereExclusive(
                User.name.neq("123") & User.name.contains("123", "234")
            )
        ) | WhereExclusive(
            (User.description == "Shit")
        )
    )
    .build_query()
)

print(statement)


# print(
#     User.select(
#         User.all_fields(),
#     ).where(
#         (User.name.not_contains(comparison_values=["123", "555", "123"]))
#         & (User.surname == "NO")
#         & User.name.between(
#             left_value="123",
#             right_value="55",
#         )
#         & User.name.not_ilike("123")
#         & User.name.ilike("123")
#     )
#     .order_by(
#         User.name,
#         ascending=False,
#         nulls_first=False,
#         order_by_statements=(
#             OrderBy(
#                 field=User.surname,
#             ),
#             OrderBy(
#                 field=User.description,
#                 ascending=False,
#                 nulls_first=False,
#             ),
#         )
#     )
#     .limit(123)
#     .build_query()
# )


# print(
#     User
#     .select(
#         [User.name, User.description],
#     )
#     .where(
#         (User.name == "Alex") & (User.description == "Meme")
#     )
#     .order_by(
#         order_by_statements=(
#             OrderBy(
#                 User.name,
#             ),
#             OrderBy(
#                 User.description,
#                 ascending=False,
#             )
#         )
#     )
#     .limit(2)
#     .build_query()
# )

# print(
#     User
#     .select(
#         [User.name, User.description],
#     )
#     .where(
#         (User.name == "Kolya") & (User.description == "Popa")
#     )
#     .order_by(
#         User.name,
#     )
#     .order_by(
#         User.description,
#         ascending=False,
#     )
#     .limit(2)
#     .build_query()
# )


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