import asyncio
from qaspen.engine.psycopg_engine import PsycopgPoolEngine
from qaspen.fields.fields import TextField, VarCharField
from qaspen.table.base_table import BaseTable


class User(BaseTable, table_name="users"):
    user_id: VarCharField = VarCharField(default="1")
    name: VarCharField = VarCharField()
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
    status: VarCharField = VarCharField()


engine = PsycopgPoolEngine(
    connection_string="postgres://postgres:12345@localhost:5432/postgres",
)

# async def run_query(query: str) -> typing.Any:
#     import psycopg
#     aconn = await psycopg.AsyncConnection.connect(
#         "postgres://postgres:12345@localhost:5432/postgres",
#     )
#     async with aconn:
#         async with aconn.cursor() as cur:
#             await cur.execute(query)
#             print(await cur.fetchall())


statement = User.select()
profile_join = (
    statement
    .join_and_return(
        based_on=User.user_id == Profile.user_id,
        fields=[
            Profile.nickname,
            Profile.description,
        ],
    )
)
video_join = (
    statement.join_and_return(
        based_on=profile_join.profile_id == Video.profile_id,
        fields=[
            Video.profile_id,
            Video.views_count,
            Video.status,
        ]
    )
)


async def main() -> None:
    await engine.startup()
    result = (
        await statement
        # .where(
        #    profile_join.nickname == "Chandr",
        # )
        .as_objects()
        .execute(engine=engine)
    )
    print(result)
    await engine.shutdown()


# async def main() -> None:
#     await run_query(query=str(query))


asyncio.run(main())

# a = User(
#     user_id="123",
# )

# print(a.name)
