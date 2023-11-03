from typing import Tuple, Type, Union

from qaspen.querystring.querystring import QueryString


class SQLType:
    """Base class for all PostgreSQL types.

    It has mapping to it's type in python.

    Usually mapped one-to-one to the Field.
    For example, `SmallInt Field` mapped to `SmallInt` type.

    But in some Fields, like `Array`, it's necessary
    to specify additional type.

    Also this and all subclasses are used in pydantic/msgspec
    converting.
    """

    python_type: Union[type, Tuple[type, ...]]

    @classmethod
    def querystring(cls: Type["SQLType"]) -> QueryString:
        return QueryString(
            cls.sql_type(),
            sql_template="{}",
        )

    @classmethod
    def sql_type(cls: Type["SQLType"]) -> str:
        return cls.__name__.upper()
