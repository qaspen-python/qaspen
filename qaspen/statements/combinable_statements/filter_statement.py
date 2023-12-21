from __future__ import annotations

import dataclasses
import functools
import operator
from typing import TYPE_CHECKING

from qaspen.querystring.querystring import FilterQueryString, QueryString
from qaspen.statements.statement import BaseStatement

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.statements.combinable_statements.combinations import (
        CombinableExpression,
    )


@dataclasses.dataclass
class FilterStatement(BaseStatement):
    """Filter statement for high-level statements.

    It is used in Select/Update/Insert/Delete Statements.

    There is `filter_operator` parameter because
    we have WHERE clauses and HAVING and them are
    almost equal.
    """

    filter_operator: str
    filter_expressions: list[CombinableExpression] = dataclasses.field(
        default_factory=list,
    )

    def add_filter(
        self: Self,
        *filter_argument: CombinableExpression,
    ) -> Self:
        """Add new CombinableExpression's.

        ### Parameters:
        - `filter_argument`: CombinableExpressions.

        ### Returns:
        `self`.
        """
        self.filter_expressions.extend(
            filter_argument,
        )
        return self

    def querystring(self: Self) -> QueryString:
        """Build new `QueryString`.

        Adds all statements to each other.

        If there is no `filter_expressions`, then
        return `EmptyQueryString`.

        ### Returns:
        `QueryString`
        """
        if not self.filter_expressions:
            return QueryString.empty()

        final_wheres = []
        for filter_expression in self.filter_expressions:
            filter_qs = filter_expression.querystring()
            final_wheres.append(
                FilterQueryString(
                    *filter_qs.template_arguments,
                    template_parameters=filter_qs.template_parameters,
                    sql_template=filter_qs.sql_template,
                ),
            )

        final_where: QueryString = functools.reduce(
            operator.add,
            final_wheres,
        )

        return QueryString(
            *final_where.template_arguments,
            sql_template=f"{self.filter_operator} {final_where.sql_template}",
            template_parameters=final_where.template_parameters,
        )
