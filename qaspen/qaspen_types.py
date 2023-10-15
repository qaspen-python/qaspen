import typing

if typing.TYPE_CHECKING:
    from qaspen.table.base_table import BaseTable


FromTable = typing.TypeVar(
    "FromTable",
    bound="BaseTable",
)
