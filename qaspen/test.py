import asyncio

from qaspen.engine.psycopgpool_engine import PsycopgPoolEngine
from qaspen.fields.datetime_fields import Date, Time, Timestamp
from qaspen.fields.integer_fields import BigInt
from qaspen.fields.string_fields import Text, VarChar
from qaspen.table.base_table import BaseTable


class User(BaseTable, table_name="users"):
    user_id: VarChar = VarChar(default="1", max_length=5)
    name: VarChar = VarChar()
    surname: VarChar = VarChar(default="Kiselev")
    description: Text = Text(default="Zopa")
    sm: VarChar = VarChar()
    created_at: Date = Date()


class Profile(BaseTable, table_name="profiles"):
    profile_id: VarChar = VarChar(default="1")
    user_id: VarChar = VarChar(default="1")
    nickname: VarChar = VarChar(default="memeLord")
    description: Text = Text(default="MemeDesc")
    number: BigInt = BigInt()


class Video(BaseTable, table_name="videos"):
    video_id: VarChar = VarChar(default="1")
    profile_id: VarChar = VarChar(default="Sasha")
    views_count: Text = Text(is_null=True)
    status: VarChar = VarChar()


class ForDate(BaseTable, table_name="for_date"):
    at_date: Date = Date()
    at_time: Time = Time()
    at_timestamp: Timestamp = Timestamp()


engine = PsycopgPoolEngine(
    connection_string="postgres://postgres:12345@localhost:5432/qaspendb",
)


async def main() -> None:
    await engine.startup()

    AliasedUser = User.aliased("CoolUser")
    AliasedProfile = Profile.aliased("CoolProfile")
    AliasedVideo = Video.aliased("CoolVideo")

    statement = (
        User.select(
            User.name,
            Profile.nickname,
            AliasedVideo.views_count,
        )
        .join(
            join_table=Profile,
            based_on=Profile.user_id == User.user_id,
        )
        .join(
            join_table=AliasedVideo,
            based_on=(Profile.profile_id == AliasedVideo.profile_id),
        )
    )

    print(statement.querystring())

    # statement = User.select(
    #     User.name,
    #     Profile.nickname,
    #     Video.views_count,
    # )
    # p_join = statement.join_and_return(
    #     join_table=Profile,
    #     based_on=Profile.user_id == User.user_id,
    # )
    # statement = statement.join(
    #     join_table=Video,
    #     based_on=Video.profile_id == p_join.profile_id,
    # )

    # statement = User.select(
    #     User.name,
    #     Profile.nickname,
    #     Video.views_count,
    # )

    # statement = statement.join(
    #     based_on=User.user_id == Profile.user_id,
    # )
    # statement = statement.join(
    #     based_on=Profile.profile_id == Video.profile_id,
    # )


asyncio.run(main())
