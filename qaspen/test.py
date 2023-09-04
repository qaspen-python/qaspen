import asyncio

from qaspen.engine.psycopg_engine import PsycopgPoolEngine
from qaspen.fields.fields import Text, VarChar
from qaspen.table.base_table import BaseTable


class User(BaseTable, table_name="users"):
    user_id: VarChar = VarChar(default="1", max_length=5)
    name: VarChar = VarChar()
    surname: VarChar = VarChar(default="Kiselev")
    description: Text = Text(default="Zopa")


class Profile(BaseTable, table_name="profiles"):
    profile_id: VarChar = VarChar(default="1")
    user_id: VarChar = VarChar(default="1")
    nickname: VarChar = VarChar(default="memeLord")
    description: Text = Text(default="MemeDesc")


class Video(BaseTable, table_name="videos"):
    video_id: VarChar = VarChar(default="1")
    profile_id: VarChar = VarChar(default="Sasha")
    views_count: VarChar = VarChar()
    status: VarChar = VarChar()


u1 = User()
u1.user_id = "555"
print("U1", u1.user_id.value)

u2 = User()
print("U2", u2.user_id.value)

# u3 = User()
# print("U3", u3.user_id._field_value)

engine = PsycopgPoolEngine(
    connection_string="postgres://postgres:12345@localhost:5432/postgres",
)

statement = User.select()
statement._from_table._table_meta.database_engine = engine
profile_join = statement.join_and_return(
    based_on=User.user_id == Profile.user_id,
    fields=[
        Profile.nickname,
        Profile.description,
    ],
)
video_join = statement.join_and_return(
    based_on=profile_join.profile_id == Video.profile_id,
    fields=[
        Video.profile_id,
        Video.views_count,
        Video.status,
    ],
)


async def main() -> None:
    await engine.startup()
    result = await statement.where(
        User.name.contains(
            subquery=User.select([User.name]).where(User.name == "Sasha"),
        ),
    )
    print(result.as_list())
    await engine.shutdown()


asyncio.run(main())
