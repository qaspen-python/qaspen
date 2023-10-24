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
    AliasedUser = User.aliased(alias="NotMe")
    AliasedProfile = Profile.aliased(alias="NotProfile")
    AliasedVideo = Video.aliased(alias="NotVideo")

    # statement = AliasedUser.select(AliasedUser.created_at)

    # statement = AliasedUser.select(AliasedUser.name)

    # j_profile = statement.join_and_return(
    #     fields=[
    #             Profile.nickname,
    #         ],
    #     based_on=Profile.user_id == Profile.user_id,
    # )

    # statement = statement.join(
    #     fields=[
    #             Video.views_count,
    #         ],
    #     based_on=Video.profile_id == j_profile.profile_id,
    # )
    # print(AliasedUser.name._field_data.from_table._table_meta.alias)
    # statement_ = AliasedUser.select(AliasedUser.name)
    # statement = statement_.union(
    #     Profile.select(Profile.nickname),
    # )

    # statement = User.select().where(
    #     ~((User.user_id == "1") & (User.name == "Sasha")) & (User.surname == "Kiselev"),
    # )

    # statement = (
    #     AliasedUser.select(AliasedUser.name)
    #     .join(
    #         fields=[
    #             AliasedProfile.nickname,
    #             AliasedProfile.number,
    #         ],
    #         based_on=AliasedProfile.user_id == AliasedUser.user_id,
    #     )
    #     .join(
    #         fields=[
    #             AliasedVideo.views_count,
    #         ],
    #         based_on=AliasedVideo.profile_id == AliasedProfile.profile_id,
    #     )
    # )
    # statement = statement.where(
    #     AliasedVideo.views_count >= "1001",
    # )

    statement = ForDate.select()

    print(statement.querystring())
    r = await statement.execute(engine=engine)
    ro = r.as_objects()
    for obj in ro:
        print(obj.at_date)
        print(type(obj.at_date))
        print(obj.at_time)
        print(type(obj.at_time))
        print(obj.at_timestamp)
        print(type(obj.at_timestamp))
    print(ro)
    # for a in ro:
    #     print(a.profiles.nickname)


asyncio.run(main())
