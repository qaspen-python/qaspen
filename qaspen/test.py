import asyncio

from psycopg_pool import AsyncConnectionPool

from qaspen.engine.psycopgpool_engine import PsycopgPoolEngine
from qaspen.fields.integer_fields import Serial
from qaspen.fields.string_fields import Text, VarChar
from qaspen.migrations.manager import MigrationManager
from qaspen.table.base_table import BaseTable
from qaspen.table.meta_table import MetaTable


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
print(MetaTable._retrieve_not_abstract_subclasses())


engine = PsycopgPoolEngine(
    connection_string="postgres://postgres:12345@localhost:5432/qaspendb",
)
mm = MigrationManager(db_engine=engine)


# async def main() -> None:
#     await engine.startup()
#     await mm.init_db()
#     # result = await statement.where(
#     #     User.name.contains(
#     #         subquery=User.select([User.name]).where(User.name == "Sasha"),
#     #     ),
#     # )
#     # print(result.as_list())
#     await engine.shutdown()


async def main() -> None:
    pool = AsyncConnectionPool(
        conninfo="postgres://postgres:12345@localhost:5432/qaspendb",
    )
    await pool.open()
    conn = await pool.getconn()
    # cur = conn.cursor()
    # cur2 = conn.cursor()

    await conn.execute(
        query="INSERT INTO for_test(name) VALUES ('500')",
    )
    await conn.execute(
        query="INSERT INTO for_test(name) VALUES ('100')",
    )


asyncio.run(main())
