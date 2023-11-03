from qaspen.sql_type.base import SQLType


class Json(SQLType):
    """Represent `JSON` PostgreSQL type in the python."""

    python_type = (dict, list)


class Jsonb(SQLType):
    """Represent `JSON` PostgreSQL type in the python."""

    python_type = (dict, list)


class Array(SQLType):
    """Represent `ARRAY` PostgreSQL type in the python."""

    python_type = list
