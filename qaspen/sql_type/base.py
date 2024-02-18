from __future__ import annotations

from qaspen.querystring.querystring import QueryString


class SQLType:
    """Base class for all PostgreSQL types.

    It has mapping to it's type in python.

    Usually mapped one-to-one to the Column.
    For example, `SmallInt Column` mapped to `SmallInt` type.

    But in some Columns, like `Array`, it's necessary
    to specify additional type.

    Also this and all subclasses are used in pydantic/msgspec
    converting.
    """

    python_type: type | tuple[type, ...]

    @classmethod
    def querystring(cls: type[SQLType]) -> QueryString:
        """Build querystring.

        Usually it equals to upper name of the class.

        ### Returns:
        new `QueryString`.
        """
        return QueryString(
            cls.sql_type(),
            sql_template="{}",
        )

    @classmethod
    def sql_type(cls: type[SQLType]) -> str:
        """Build string Type in PostgreSQL.

        ### Returns:
        string.
        """
        return cls.__name__.upper()
