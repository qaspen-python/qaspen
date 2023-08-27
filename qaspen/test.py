from qaspen.fields.fields import TextField, VarCharField
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.join_statement import Join, JoinStatement
from qaspen.statements.combinable_statements.order_by_statement import OrderBy
from qaspen.statements.combinable_statements.where_statement import (
    WhereExclusive,
)
from qaspen.statements.select_statement import SelectStatement
from qaspen.table.base_table import BaseTable


class User(BaseTable, table_name="users"):
    user_id: VarCharField = VarCharField(default="1")
    name: VarCharField = VarCharField(default="Sasha")
    surname: VarCharField = VarCharField(default="Kiselev")
    description: TextField = TextField(default="Zopa")


class Profile(BaseTable, table_name="profiles"):
    profile_id: VarCharField = VarCharField(default="1")
    user_id: VarCharField = VarCharField(default="1")
    nickname: VarCharField = VarCharField(default="memeLord")
    description: TextField = TextField(default="MemeDesc")


class Video(BaseTable, table_name="videos"):
    video_id: VarCharField = VarCharField(default="1")
    profile_id: VarCharField = VarCharField(default="Sasha")


# statement1 = (
#     User.select(User.all_fields())
#     .join_on(
#         fields=[
#             Profile.profile_id,
#             Profile.nickname,
#         ],
#         based_on=Profile.user_id == User.user_id,
#     )
# )

# print(statement1.querystring())


statement2 = User.select(User.all_fields())
profile_join = Join(
    fields=[
        Profile.profile_id,
        Profile.nickname,
    ],
    from_table=User,
    join_table=Profile,
    on=Profile.user_id == User.user_id,
    join_alias="profile_join",
)
video_join = Join(
    fields=[
        Video.video_id,
    ],
    from_table=User,
    join_table=Video,
    on=(
        WhereExclusive(
            (profile_join.profile_id == Video.profile_id)
            & (profile_join.profile_id == User.user_id)
        )
        | (profile_join.profile_id == Profile.profile_id)
    ),
    join_alias="video_join",
)

statement2.add_join(
    profile_join,
    video_join,
)

print(statement2.querystring())
