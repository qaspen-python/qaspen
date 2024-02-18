from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final, Generic

from qaspen.base.sql_base import SQLComparison
from qaspen.clauses.filter import Filter, FilterBetween
from qaspen.columns import operators
from qaspen.exceptions import FilterComparisonError
from qaspen.qaspen_types import ComparisonT

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.base.sql_base import SQLSelectable


class EqualComparisonMixin(SQLComparison[ComparisonT]):
    """Mixin class to provide `==` comparison."""

    def __eq__(  # type: ignore[override]
        self: Self,
        comparison: ComparisonT,
    ) -> Filter:
        if comparison is None:
            return Filter(
                left_operand=self,
                operator=operators.IsNullOperator,
            )

        return Filter(
            left_operand=self,
            comparison_value=comparison,
            operator=operators.EqualOperator,
        )

    def eq(self: Self, comparison: ComparisonT) -> Filter:
        """Analog for `==` (`__eq__` method) operation.

        Works exactly the same. It exists just because some
        people prefer to use methods instead of python comparison.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        return self.__eq__(
            comparison,
        )


class NotEqualComparisonMixin(SQLComparison[ComparisonT]):
    """Mixin class to provide `!=` comparison."""

    def __ne__(  # type: ignore[override]
        self: Self,
        comparison_value: ComparisonT,
    ) -> Filter:
        if comparison_value is None:
            return Filter(
                left_operand=self,
                operator=operators.IsNotNullOperator,
            )

        return Filter(
            left_operand=self,
            comparison_value=comparison_value,
            operator=operators.NotEqualOperator,
        )

    def neq(
        self: Self,
        comparison_value: ComparisonT,
    ) -> Filter:
        """Analog for `!=` (`__ne__` method) operation.

        Works exactly the same. It exists just because some
        people prefer to use methods instead of python comparison.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        return self.__ne__(comparison_value)


class GreaterComparisonMixin(SQLComparison[ComparisonT]):
    """Mixin class to provide `>` comparison."""

    def __gt__(
        self: Self,
        comparison_value: ComparisonT,
    ) -> Filter:
        return Filter(
            left_operand=self,
            comparison_value=comparison_value,
            operator=operators.GreaterOperator,
        )

    def gt(
        self: Self,
        comparison_value: ComparisonT,
    ) -> Filter:
        """Analog for `>` (`__gt__` method) operation.

        Works exactly the same. It exists just because some
        people prefer to use methods instead of python comparison.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        return self.__gt__(comparison_value)


class GreaterEqualComparisonMixin(SQLComparison[ComparisonT]):
    """Mixin class to provide `>=` comparison."""

    def __ge__(
        self: Self,
        comparison_value: ComparisonT,
    ) -> Filter:
        return Filter(
            left_operand=self,
            comparison_value=comparison_value,
            operator=operators.GreaterEqualOperator,
        )

    def gte(
        self: Self,
        comparison_value: ComparisonT,
    ) -> Filter:
        """Analog for `>=` (`__ge__` method) operation.

        Works exactly the same. It exists just because some
        people prefer to use methods instead of python comparison.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        return self.__ge__(comparison_value)


class LessComparisonMixin(SQLComparison[ComparisonT]):
    """Mixin class to provide `<` comparison."""

    def __lt__(
        self: Self,
        comparison_value: ComparisonT,
    ) -> Filter:
        return Filter(
            left_operand=self,
            comparison_value=comparison_value,
            operator=operators.LessOperator,
        )

    def lt(
        self: Self,
        comparison_value: ComparisonT,
    ) -> Filter:
        """Analog for `<` (`__lt__` method) operation.

        Works exactly the same. It exists just because some
        people prefer to use methods instead of python comparison.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        return self.__lt__(comparison_value)


class LessEqualComparisonMixin(SQLComparison[ComparisonT]):
    """Mixin class to provide `<=` comparison."""

    def __le__(
        self: Self,
        comparison_value: ComparisonT,
    ) -> Filter:
        return Filter(
            left_operand=self,
            comparison_value=comparison_value,
            operator=operators.LessEqualOperator,
        )

    def lte(
        self: Self,
        comparison_value: ComparisonT,
    ) -> Filter:
        """Analog for `<=` (`__le__` method) operation.

        Works exactly the same. It exists just because some
        people prefer to use methods instead of python comparison.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        return self.__le__(comparison_value)


class BetweenComparisonMixin(SQLComparison[ComparisonT]):
    """Mixin class to provide `BETWEEN` comparison."""

    def between(
        self: Self,
        left_value: ComparisonT,
        right_value: ComparisonT,
    ) -> FilterBetween:
        """`BETWEEN` PostgreSQL clause.

        It allows you to use `BETWEEN` clause.

        ### Parameters:
        - `left_value`: left-side value in the between clause.
        - `right_value`: right-side value in the between clause.

        ### Returns:
        Initialized `FilterBetween`.
        """
        return FilterBetween(
            column=self,
            operator=operators.BetweenOperator,
            left_comparison_value=left_value,
            right_comparison_value=right_value,
        )


class InComparisonMixin(SQLComparison[ComparisonT]):
    """Mixin class to provide `IN` comparison."""

    def in_(
        self: Self,
        *comparison_values: ComparisonT,
        subquery: SQLSelectable | None = None,
    ) -> Filter:
        """`IN` PostgreSQL clause.

        It allows you to use `IN` clause.
        You can specify either unlimited number of `comparison` values
        or `subquery`.

        ### Parameters:
        - `comparison_values`: values for `IN` clause,
            they must be correct type. For example, if you are
            working with string Column, you have to use str objects.
        - `subquery`: Any object that provides `queryset()` method.

        ### Returns:
        Initialized `Filter` class.

        Example:
        -------
        ```python
        class Buns(BaseTable, table_name="buns"):
            name: VarCharColumn = VarCharColumn()


        select_statement = (
            Buns
            .select()
            .where(
                Buns.name.in_(
                    "Awesome",
                    "Qaspen",
                    "Project",
                )
            )
        )

        select_statement = (
            Buns
            .select()
            .where(
                Buns.name.in_(
                    subquery=Buns.select(
                        Buns.name,
                    ),
                ),
            )
        )
        ```
        """
        if subquery and comparison_values:
            args_err_msg: Final = (
                "It's not possible to specify subquery "
                "with positional arguments in `in_` method. "
                "Please choose either subquery or positional arguments.",
            )
            raise FilterComparisonError(args_err_msg)

        where_parameters: dict[str, Any] = {
            "left_operand": self,
            "operator": operators.InOperator,
        }

        if subquery:
            where_parameters["comparison_value"] = subquery
        elif comparison_values:
            where_parameters["comparison_values"] = list(comparison_values)
            where_parameters["operator"] = operators.AnyOperator

        return Filter(**where_parameters)


class NotInComparisonMixin(SQLComparison[ComparisonT]):
    """Mixin class to provide `NOT IN` comparison."""

    def not_in(
        self: Self,
        *comparison_values: ComparisonT,
        subquery: SQLSelectable | None = None,
    ) -> Filter:
        """`NOT IN` PostgreSQL clause.

        It allows you to use `NOT IN` clause.
        You can specify either unlimited number of `comparison` values
        or `subquery`.

        ### Parameters:
        - `comparison_values`: values for `NOT IN` clause,
            they must be correct type. For example, if you are
            working with string Column, you have to use str objects.
        - `subquery`: Any object that provides `queryset()` method.

        ### Returns:
        Initialized `Filter` class.

        Example:
        -------
        ```python
        class Buns(BaseTable, table_name="buns"):
            name: VarCharColumn = VarCharColumn()


        select_statement = (
            Buns
            .select()
            .where(
                Buns.name.not_in(
                    "Awesome",
                    "Qaspen",
                    "Project",
                )
            )
        )

        select_statement = (
            Buns
            .select()
            .where(
                Buns.name.not_in(
                    subquery=Buns.select(
                        Buns.name,
                    ),
                ),
            )
        )
        ```
        """
        if subquery and comparison_values:
            args_err_msg: Final = (
                "It's not possible to specify subquery "
                "with positional arguments in `not_in` method. "
                "Please choose either subquery or positional arguments.",
            )
            raise FilterComparisonError(args_err_msg)

        where_parameters: dict[str, Any] = {
            "left_operand": self,
            "operator": operators.NotInOperator,
        }

        if subquery:
            where_parameters["comparison_value"] = subquery
        elif comparison_values:
            where_parameters["comparison_values"] = list(comparison_values)
            where_parameters["operator"] = operators.NotAnyOperator

        return Filter(**where_parameters)


class LikeComparisonMixin(SQLComparison[ComparisonT]):
    """Mixin class to provide `LIKE` comparison."""

    def like(
        self: Self,
        comparison_value: ComparisonT,
    ) -> Filter:
        """`LIKE` PostgreSQL clause.

        It allows you to use `LIKE` clause.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        return Filter(
            left_operand=self,
            comparison_value=comparison_value,
            operator=operators.LikeOperator,
        )


class NotLikeComparisonMixin(SQLComparison[ComparisonT]):
    """Mixin class to provide `NOT LIKE` comparison."""

    def not_like(
        self: Self,
        comparison_value: ComparisonT,
    ) -> Filter:
        """`NOT LIKE` PostgreSQL clause.

        It allows you to use `NOT LIKE` clause.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        return Filter(
            left_operand=self,
            comparison_value=comparison_value,
            operator=operators.NotLikeOperator,
        )


class ILikeComparisonMixin(SQLComparison[ComparisonT]):
    """Mixin class to provide `ILIKE` comparison."""

    def ilike(
        self: Self,
        comparison_value: ComparisonT,
    ) -> Filter:
        """`ILIKE` PostgreSQL clause.

        It allows you to use `ILIKE` clause.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        return Filter(
            left_operand=self,
            comparison_value=comparison_value,
            operator=operators.ILikeOperator,
        )


class NotILikeComparisonMixin(SQLComparison[ComparisonT]):
    """Mixin class to provide `NOT ILIKE` comparison."""

    def not_ilike(
        self: Self,
        comparison_value: ComparisonT,
    ) -> Filter:
        """`NOT ILIKE` PostgreSQL clause.

        It allows you to use `NOT ILIKE` clause.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        return Filter(
            left_operand=self,
            comparison_value=comparison_value,
            operator=operators.NotILikeOperator,
        )


class AllComparisonMixin(
    EqualComparisonMixin[ComparisonT],
    NotEqualComparisonMixin[ComparisonT],
    GreaterComparisonMixin[ComparisonT],
    GreaterEqualComparisonMixin[ComparisonT],
    LessComparisonMixin[ComparisonT],
    LessEqualComparisonMixin[ComparisonT],
    BetweenComparisonMixin[ComparisonT],
    InComparisonMixin[ComparisonT],
    NotInComparisonMixin[ComparisonT],
    LikeComparisonMixin[ComparisonT],
    NotLikeComparisonMixin[ComparisonT],
    ILikeComparisonMixin[ComparisonT],
    NotILikeComparisonMixin[ComparisonT],
    Generic[ComparisonT],
):
    """Mixin with all comparisons.

    It's used in `Text` class and in all aggregate functions.
    """
