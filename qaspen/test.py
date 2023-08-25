from qaspen.fields.fields import TextField, VarCharField
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.join_statement import JoinStatement
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


class Profile(BaseTable, table_name="profiles"):
    user: VarCharField = VarCharField(default="nana")
    nickname: VarCharField = VarCharField(default="memeLord")
    description: TextField = TextField(default="MemeDesc")


class Video(BaseTable, table_name="videos"):
    user: VarCharField = VarCharField(default="Sasha")
    video_id: VarCharField = VarCharField(default="1")


print(
    User.select(User.all_fields())
    .join(
        fields_to_join=[
            Profile.nickname,
        ],
        based_on=User.name.between(
            left_value=User.name,
            right_value=Profile.user,
        )
    )
    .where(
        User.description == "Meme"
    )
    .order_by(User.name)
    .limit_offset(
        limit=10,
        offset=0,
    )
    .make_sql_string()
)
