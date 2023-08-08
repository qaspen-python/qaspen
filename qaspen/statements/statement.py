import abc
import dataclasses
from qaspen.fields.comparisons import Where


@dataclasses.dataclass
class Statement(abc.ABC):
    """Dataclass for all SQL queries."""
    where_statements: list[Where]
