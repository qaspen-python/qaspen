from __future__ import annotations

import functools
import operator
from typing import TYPE_CHECKING, Any, Final, Generic, Sequence, TypeVar

from qaspen.columns.base import Column
from qaspen.qaspen_types import EMPTY_FIELD_VALUE, FromTable
from qaspen.querystring.querystring import (
    FullStatementQueryString,
    QueryString,
)
from qaspen.statements.base import Executable
from qaspen.statements.statement import BaseStatement

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.abc.db_engine import BaseEngine
    from qaspen.abc.db_transaction import BaseTransaction


ReturnResultType = TypeVar(
    "ReturnResultType",
)
ReturningColumn = TypeVar(
    "ReturningColumn",
    bound=Column[Any],
)


class BaseInsertStatement(
    BaseStatement,
    Executable[ReturnResultType],
    Generic[FromTable, ReturnResultType],
):
    """Base class for all InsertStatements."""

    def __init__(self: Self, from_table: type[FromTable]) -> None:
        self._from_table: Final = from_table
        self._returning_column: Column[Any] | None = None

    async def execute(
        self: Self,
        engine: BaseEngine[Any, Any, Any],
    ) -> ReturnResultType:
        """Execute select statement.

        This is manual execution.
        You can pass specific engine.

        ### Parameters
        - `engine`: subclass of BaseEngine.

        ### Returns
        `SelectStatementResult`
        """
        querystring, qs_parameters = self.querystring().build()
        raw_query_result: list[dict[str, Any]] | None = await engine.execute(
            querystring=querystring,
            querystring_parameters=qs_parameters,
            fetch_results=bool(self._returning_column),
        )

        return self._parse_raw_query_result(
            raw_query_result=raw_query_result,
        )

    async def transaction_execute(
        self: Self,
        transaction: BaseTransaction[Any, Any],
    ) -> ReturnResultType:
        """Execute statement inside a transaction context.

        This is manual execution.
        You can pass specific transaction.
        IMPORTANT: To commit the changes, with this way of execution,
        it's necessary to manually call `await transaction.commit()`.

        ### Parameters:
        - `transaction`: running transaction.
        database response or not.

        ### Returns
        `InsertStatement`
        """
        querystring, qs_parameters = self.querystring().build()
        raw_query_result: (
            list[dict[str, Any]] | None
        ) = await transaction.execute(
            querystring=querystring,
            querystring_parameters=qs_parameters,
            fetch_results=bool(self._returning_column),
        )

        return self._parse_raw_query_result(
            raw_query_result=raw_query_result,
        )

    def returning(
        self: Self,
        return_column: ReturningColumn,
    ) -> InsertStatement[FromTable, list[ReturningColumn]]:
        """Add `RETURNING` to the query.

        ### Parameters:
        - `return_column`: column to return

        ### Returns:
        `self` with new return type.
        """
        self._returning_column = return_column
        return self  # type: ignore[return-value]

    def _parse_raw_query_result(
        self: Self,
        raw_query_result: list[dict[str, Any]] | None,
    ) -> ReturnResultType:
        if not self._returning_column or not raw_query_result:
            return None  # type: ignore[return-value]

        return [  # type: ignore[return-value]
            db_record[self._returning_column._original_column_name]
            for db_record in raw_query_result
        ]


class InsertStatement(BaseInsertStatement[FromTable, ReturnResultType]):
    """Main entry point for all INSERT queries."""

    def __init__(
        self: Self,
        from_table: type[FromTable],
        columns_to_insert: list[Column[Any]],
        values_to_insert: tuple[list[Any], ...],
    ) -> None:
        super().__init__(from_table=from_table)

        self._not_passed_columns_with_default = (
            self._find_not_passed_column_with_default(
                columns_to_insert=columns_to_insert,
            )
        )
        self._columns_to_insert: Final = (
            columns_to_insert + self._not_passed_columns_with_default
        )

        self._values_to_insert: Final = values_to_insert

        self._returning_column: Column[Any] | None = None

    def querystring(self: Self) -> QueryString:
        """Build querystring for INSERT statement."""
        returning_qs = (
            QueryString(
                self._returning_column._original_column_name,
                sql_template=f"RETURNING {QueryString.arg_ph()}",
            )
            if self._returning_column
            else QueryString.empty()
        )

        return QueryString(
            self._from_table.table_name(),
            self._make_columns_querystring(),
            self._make_values_querystring(),
            returning_qs,
            sql_template=(
                f"INSERT INTO {QueryString.arg_ph()}"
                f"{QueryString.arg_ph()} "
                f"VALUES {QueryString.arg_ph()} {QueryString.arg_ph()}"
            ),
        )

    def _find_not_passed_column_with_default(
        self: Self,
        columns_to_insert: list[Column[Any]],
    ) -> list[Column[Any]]:
        """Find all not passed table columns with default values.

        ### Parameters:
        - `columns_to_insert`: user-passed columns.

        ### Returns:
        not passed into `InsertStatement` columns
        with default values.
        """
        all_columns_with_default = (
            self._from_table._columns_with_default().values()
        )
        set_columns_to_insert = {
            column._original_column_name for column in columns_to_insert
        }

        return [
            column
            for column in all_columns_with_default
            if column._original_column_name not in set_columns_to_insert
        ]

    def _prepare_insert_values(
        self: Self,
        values_to_insert: tuple[list[Any], ...],
    ) -> tuple[list[Any], ...]:
        """Prepare INSERT values.

        We need to process `values_to_insert` and
        add to them default value of the columns
        with `default` or `callable_default`.

        ### Parameters:
        - `values_to_insert`: user-passed values to insert.

        ### Returns:
        tuple of lists with values to insert.
        """
        for values_to_insert_list in values_to_insert:
            for element_idx, element in enumerate(values_to_insert_list):
                values_to_insert_list[element_idx] = element

        if not self._not_passed_columns_with_default:
            return values_to_insert

        for list_value in values_to_insert:
            for column_with_default in self._not_passed_columns_with_default:
                if column_with_default._default:
                    list_value.append(
                        column_with_default._default,
                    )
                elif column_with_default._callable_default:
                    list_value.append(
                        column_with_default._callable_default(),
                    )

        return values_to_insert

    def _make_columns_querystring(self: Self) -> QueryString:
        """Create `QueryString` for columns that will be inserted.

        ### Returns:
        `Querystring` for columns in INSERT SQL.
        """
        columns_sql_template = ", ".join(
            [QueryString.arg_ph()] * len(self._columns_to_insert),
        )

        columns_sql_template_args = [
            column_to_insert._original_column_name
            for column_to_insert in self._columns_to_insert
        ]

        return QueryString(
            *columns_sql_template_args,
            sql_template="(" + columns_sql_template + ")",
        )

    def _make_values_querystring(self: Self) -> QueryString:
        """Create `QueryString` for VALUES that will be inserted.

        Firstly we prepare `values_to_insert`, we can't do it
        before this method because default values may contain
        callable objects based on date, time, etc.
        So we process them just before execution.

        ### Returns:
        `Querystring` for VALUES in INSERT SQL.
        """
        all_values_to_insert = self._prepare_insert_values(
            values_to_insert=self._values_to_insert,
        )

        values_sql_template = ", ".join(
            [QueryString.param_ph()] * len(all_values_to_insert),
        )

        single_values_template = ", ".join(
            [QueryString.param_ph()] * len(all_values_to_insert[0]),
        )
        values_sql_template_params = [
            QueryString(
                template_parameters=values_record,
                sql_template="(" + single_values_template + ")",
            )
            for values_record in all_values_to_insert
        ]
        return QueryString(
            template_parameters=values_sql_template_params,
            sql_template=values_sql_template,
        )


class InsertObjectsStatement(
    BaseInsertStatement[FromTable, ReturnResultType],
):
    """Main entry point for all INSERT queries based on python objects."""

    def __init__(
        self: Self,
        from_table: type[FromTable],
        insert_objects: Sequence[FromTable],
    ) -> None:
        super().__init__(from_table=from_table)
        self._insert_objects: Final = insert_objects

        self._returning_column: Column[Any] | None = None

    async def execute(
        self: Self,
        engine: BaseEngine[Any, Any, Any],
    ) -> ReturnResultType:
        """Execute select statement.

        This is manual execution.
        You can pass specific engine.

        ### Parameters
        - `engine`: subclass of BaseEngine.

        ### Returns
        `SelectStatementResult`
        """
        returned_values = []
        all_qs_objects = [
            self._build_object_querystring(table_object)
            for table_object in self._insert_objects
        ]
        for qs_object in all_qs_objects:
            querystring, qs_parameters = qs_object.build()
            raw_query_result: (
                list[dict[str, Any]] | None
            ) = await engine.execute(
                querystring=querystring,
                querystring_parameters=qs_parameters,
                fetch_results=bool(self._returning_column),
            )

            returned_value: list[Any] = self._parse_raw_query_result(  # type: ignore[assignment]
                raw_query_result=raw_query_result,
            )

            if self._returning_column and returned_value:
                returned_values.append(returned_value[0])

        if self._returning_column:
            return returned_values  # type: ignore[return-value]

        return None  # type: ignore[return-value]

    async def transaction_execute(
        self: Self,
        transaction: BaseTransaction[Any, Any],
    ) -> ReturnResultType:
        """Execute statement inside a transaction context.

        This is manual execution.
        You can pass specific transaction.
        IMPORTANT: To commit the changes, with this way of execution,
        it's necessary to manually call `await transaction.commit()`.

        ### Parameters:
        - `transaction`: running transaction.
        database response or not.

        ### Returns
        `InsertStatement`
        """
        returned_values = []
        all_qs_objects = [
            self._build_object_querystring(table_object)
            for table_object in self._insert_objects
        ]

        for qs_object in all_qs_objects:
            querystring, qs_parameters = qs_object.build()
            raw_query_result: (
                list[dict[str, Any]] | None
            ) = await transaction.execute(
                querystring=querystring,
                querystring_parameters=qs_parameters,
                fetch_results=bool(self._returning_column),
            )

            returned_value: list[Any] = self._parse_raw_query_result(  # type: ignore[assignment]
                raw_query_result=raw_query_result,
            )

            if self._returning_column and returned_value:
                returned_values.append(returned_value[0])

        if self._returning_column:
            return returned_values  # type: ignore[return-value]

        return None  # type: ignore[return-value]

    def querystring(self: Self) -> QueryString:
        """Build querystring for INSERT statement."""
        objects_insert_querystrings: list[FullStatementQueryString] = [
            self._build_object_querystring(table_object)
            for table_object in self._insert_objects
        ]

        final_querystring: QueryString = functools.reduce(
            operator.add,
            objects_insert_querystrings,
        )

        return final_querystring

    def _build_object_querystring(
        self: Self,
        table_object: FromTable,
    ) -> FullStatementQueryString:
        """Build querystring for single object.

        ### Parameters:
        - `table_object`: Table instance to insert.

        ### Returns:
        new generated `QueryString`.
        """
        columns_to_insert = self._retrieve_object_columns_to_insert(
            table_object,
        )

        insert_columns_qs = QueryString(
            *[column._original_column_name for column in columns_to_insert],
            sql_template="("
            + ", ".join(["{}" for _ in columns_to_insert])
            + ")",
        )

        values_to_insert_qs = QueryString(
            template_parameters=self._prepare_values_to_insert(
                columns_to_insert,
            ),
            sql_template="("
            + ", ".join([QueryString.param_ph()] * len(columns_to_insert))
            + ")",
        )

        returning_qs = (
            QueryString(
                self._returning_column._original_column_name,
                sql_template=f" RETURNING {QueryString.arg_ph()}",
            )
            if self._returning_column
            else QueryString.empty()
        )

        return FullStatementQueryString(
            table_object.original_table_name(),
            insert_columns_qs,
            values_to_insert_qs,
            returning_qs,
            sql_template="INSERT INTO {}{} VALUES {}{}",
        )

    def _retrieve_object_columns_to_insert(
        self: Self,
        table_object: FromTable,
    ) -> list[Column[Any]]:
        """Find all columns that must be inserted.

        That means that if columns with default value on
        database level haven't passed we shouldn't specify them.
        """
        result_columns: list[Column[Any]] = []
        column: Column[Any]
        for column in table_object._table_meta.table_columns.values():
            available_condition: bool = any(
                (
                    column._column_data.column_value != EMPTY_FIELD_VALUE,
                    column._column_data.callable_default,
                    column._column_data.default,
                ),
            )
            if available_condition:
                result_columns.append(column)

        return result_columns

    def _prepare_values_to_insert(
        self: Self,
        table_columns: list[Column[Any]],
    ) -> list[Any]:
        """Prepare INSERT values.

        ### Parameters:
        - `values_to_insert`: user-passed values to insert.

        ### Returns:
        tuple of lists with values to insert.
        """
        return [
            column._column_data.column_value
            for column in table_columns
            if column._column_data.column_value != EMPTY_FIELD_VALUE
        ]
