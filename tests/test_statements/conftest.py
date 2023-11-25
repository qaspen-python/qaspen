from qaspen.fields.primitive import SerialField, TextField
from qaspen.table.base_table import BaseTable


class ForTestTable(BaseTable):
    """Class for test purposes."""

    name: TextField = TextField()
    count: TextField = TextField()


class UserTest(BaseTable):
    """Class for testing joins."""

    id: SerialField = SerialField()  # noqa: A003
    description: TextField = TextField()


class VideoTest(BaseTable):
    """Class for testing joins."""

    user_id: SerialField = SerialField()
    video_id: SerialField = SerialField()
