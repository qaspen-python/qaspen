import abc
import dataclasses

from qaspen.statements.combinable_statements.where_statement import Where


@dataclasses.dataclass
class Statement(abc.ABC):
    """Dataclass for all SQL queries."""
    where_statements: list[Where]
