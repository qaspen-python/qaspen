import asyncio

from qaspen.engine.psycopgpool_engine import PsycopgPoolEngine
from qaspen.fields.string_fields import Text, VarChar
from qaspen.table.base_table import BaseTable


class User(BaseTable, table_name="users"):
    user_id: VarChar = VarChar(default="1", max_length=5)
    name: VarChar = VarChar()
    surname: VarChar = VarChar(default="Kiselev")
    description: Text = Text(default="Zopa")
    sm: VarChar = VarChar()


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


engine = PsycopgPoolEngine(
    connection_string="postgres://postgres:12345@localhost:5432/qaspendb",
)


async def main() -> None:
    await engine.startup()
    AliasedUser = User.aliased(alias="NotMe")
    AliasedProfile = Profile.aliased(alias="NotProfile")
    AliasedVideo = Video.aliased(alias="NotVideo")

    statement = (
        AliasedUser.select()
        .left_join(
            fields=[
                AliasedProfile.nickname,
            ],
            based_on=AliasedProfile.user_id == AliasedUser.user_id,
        )
        .left_join(
            fields=[
                AliasedVideo.profile_id,
            ],
            based_on=AliasedProfile.profile_id == AliasedVideo.profile_id,
        )
    )
    r = await statement.execute(engine=engine)
    print(r.as_list())
    user_r = r.as_objects()[0]

    await engine.startup()


asyncio.run(main())
