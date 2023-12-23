from typing_extensions import Self

from qaspen.base.sql_base import SQLComparison
from qaspen.clauses.filter import Filter
from qaspen.fields.operators import EqualOperator
from qaspen.qaspen_types import ComparisonT


class EqualOperatorMixin(SQLComparison[ComparisonT]):
    def __eq__(  # type: ignore[override]
        self: Self,
        comparison: ComparisonT,
    ) -> Filter:
        return Filter(
            left_operand=self,
            comparison_value=comparison,
            operator=EqualOperator,
        )

    def eq(
        self: Self,
        comparison: ComparisonT,
    ) -> Filter:
        return self.__eq__(
            comparison,
        )
