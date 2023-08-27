from qaspen.fields.fields import TextField, VarCharField
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.join_statement import InnerJoin, Join, JoinStatement, RightOuterJoin
from qaspen.statements.combinable_statements.order_by_statement import OrderBy
from qaspen.statements.combinable_statements.filter_statement import (
    FilterExclusive,
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
    views_count: VarCharField = VarCharField(default="20")


print(
    User.select([User.name]).querystring()
)

statement = User.select()

profile_join = Join(
    fields=[Profile.nickname, Profile.description],
    from_table=User,
    join_table=Profile,
    on=Profile.user_id == User.user_id,
    join_alias="profile_join",
)

video_join = Join(
    fields=[Video.views_count],
    from_table=User,
    join_table=Video,
    on=profile_join.profile_id == Video.profile_id,
    join_alias="video_join",
)

statement.add_join(
    profile_join,
    video_join,
)

statement.where(
    video_join.views_count > "100",
)

print(statement.querystring())


statement2 = User.select()

profile_join_2 = statement2.join_on_with_return(
    based_on=Profile.user_id == User.user_id,
    fields=[Profile.nickname, Profile.description],
)

video_join_2 = statement2.left_join_on_with_return(
    based_on=profile_join_2.profile_id == Video.profile_id,
    fields=[Video.views_count],
)

statement2.where(video_join_2.views_count > "100")

print(statement2.querystring())