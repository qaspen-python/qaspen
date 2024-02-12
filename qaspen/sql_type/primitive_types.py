from __future__ import annotations

import datetime

from qaspen.sql_type.base import SQLType


class SmallInt(SQLType):
    """Represent `SMALLINT` PostgreSQL type in the python."""

    python_type = int


class Integer(SQLType):
    """Represent `INTEGER` PostgreSQL type in the python."""

    python_type = int


class BigInt(SQLType):
    """Represent `BigInt` PostgreSQL type in the python."""

    python_type = int


class Numeric(SQLType):
    """Represent `Numeric` PostgreSQL type in the python."""

    python_type = int


class Decimal(SQLType):
    """Represent `Decimal` PostgreSQL type in the python."""

    python_type = int


class Real(SQLType):
    """Represent `REAL` PostgreSQL type in the python."""

    python_type = (float, str)


class DoublePrecision(SQLType):
    """Represent `DOUBLEPRECISION` PostgreSQL type in the python."""

    python_type = (float, str)


class Boolean(SQLType):
    """Represent `BOOLEAN` PostgreSQL type in the python."""

    python_type = bool


class VarChar(SQLType):
    """Represent `VARCHAR` PostgreSQL type in the python."""

    python_type = str


class Text(SQLType):
    """Represent `TEXT` PostgreSQL type in the python."""

    python_type = str


class Char(SQLType):
    """Represent `CHAR` PostgreSQL type in the python."""

    python_type = str


class Date(SQLType):
    """Represent `DATE` PostgreSQL type in the python."""

    python_type = datetime.date


class Time(SQLType):
    """Represent `TIME` PostgreSQL type in the python."""

    python_type = datetime.time


class TimeTZ(SQLType):
    """Represent `TIME` PostgreSQL type in the python."""

    python_type = datetime.time

    @classmethod
    def sql_type(cls: type[SQLType]) -> str:
        """Build string Type in PostgreSQL.

        ### Returns:
        string.
        """
        return "TIME WITH TIME ZONE"


class Timestamp(SQLType):
    """Represent `TIMESTAMP` PostgreSQL type in the python."""

    python_type = datetime.datetime


class TimestampTZ(SQLType):
    """Represent `TIMESTAMP` PostgreSQL type in the python."""

    python_type = datetime.datetime

    @classmethod
    def sql_type(cls: type[SQLType]) -> str:
        """Build string Type in PostgreSQL.

        ### Returns:
        string.
        """
        return "TIMESTAMP WITH TIME ZONE"


class Interval(SQLType):
    """Represent `INTERVAL` PostgreSQL type in the python."""

    python_type = datetime.timedelta
