import asyncio

from qaspen.engine.psycopg_engine import PsycopgPoolEngine
from qaspen.fields.integer_fields import Serial
from qaspen.fields.string_fields import Text, VarChar
from qaspen.migrations.manager import MigrationManager
from qaspen.table.base_table import BaseTable


class User(BaseTable, table_name="users"):
    user_id: VarChar = VarChar(default="1", max_length=5)
    name: VarChar = VarChar()
    surname: VarChar = VarChar(default="Kiselev")
    description: Text = Text(default="Zopa")
    sm = Serial()


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


print(User.sm._make_field_create_statement())


engine = PsycopgPoolEngine(
    connection_string="postgres://postgres:12345@localhost:5432/qaspendb",
)
mm = MigrationManager(db_engine=engine)


async def main() -> None:
    await engine.startup()
    await mm.init_db()
    # result = await statement.where(
    #     User.name.contains(
    #         subquery=User.select([User.name]).where(User.name == "Sasha"),
    #     ),
    # )
    # print(result.as_list())
    await engine.shutdown()


asyncio.run(main())
