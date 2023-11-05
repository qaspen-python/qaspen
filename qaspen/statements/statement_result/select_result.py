from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Final, Generic, List

from qaspen.qaspen_types import FromTable
from qaspen.statements.statement_result.result_variants import (
    ListableStatementResult,
    ObjecttableStatementResult,
    StatementResult,
)

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.fields.aliases import FieldAliases
    from qaspen.table.base_table import BaseTable


class SelectStatementResult(
    StatementResult,
    ListableStatementResult[List[Dict[str, Any]]],
    ObjecttableStatementResult[FromTable],
    Generic[FromTable],
):
    """Result for SelectStatement.

    It has different variants of result display.
    - `as_list()` - return list of dicts.
    - `as_objects()` - return list of objects.
    - `raw_result()` - return result from engine.
    """

    def __init__(
        self: Self,
        from_table: type[FromTable],
        query_result: list[tuple[Any, ...]],
        aliases: FieldAliases,
    ) -> None:
        self.from_table: Final = from_table
        self.query_result: Final = query_result
        self.aliases: Final = aliases

    def as_list(self: Self) -> list[dict[str, Any]]:
        """Return list of dicts.

        Example:
        -------
        ```python
        # Assume that each table has only one row.
        import asyncio
        from qaspen import BaseTable


        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        class Cookies(BaseTable, table_name="cookies"):
            bun_name: VarCharField = VarCharField()
            filling: VarCharField = VarCharField()
            topping: VarCharField = VarCharField()

        statement = (
            Buns
            .select(Buns.name)
            .inner_join(
                fields=[
                    Cookies.filling,
                    Cookies.topping,
                ],
                based_on=Buns.name == Cookies.bun_name,
            )
        )


        async def main() -> None:
            result = await statement
            print(result.as_list())

        # This will produce this structure list:
        [
            {
                "name": <Name of the Buns>,
                "_cookies": {
                    "filling": <cookie filling>,
                    "topping": <cookie topping>,
                },
            },
            ...
        ]
        ```
        """
        result_list: list[dict[str, Any]] = []

        for single_query_result in self.query_result:
            zip_expression = zip(
                single_query_result,
                self.aliases.values(),
            )

            result_list.append({})

            for single_query_record, field in zip_expression:
                is_from_table: bool = (
                    field.aliased_field._field_data.from_table
                    is self.from_table
                )

                result_dict = result_list[-1]

                if is_from_table:
                    result_dict[
                        field.aliased_field._original_field_name
                    ] = single_query_record
                else:
                    joined_results = result_dict.setdefault(
                        f"_{field.aliased_field._table_name}",
                        {},
                    )
                    joined_results[
                        field.aliased_field._original_field_name
                    ] = single_query_record

        return result_list

    def as_objects(self: Self) -> list[FromTable]:
        """Return list of objects.

        Example:
        -------
        ```python
        # Assume that each table has only one row.
        import asyncio
        from qaspen import BaseTable


        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        class Cookies(BaseTable, table_name="cookies"):
            bun_name: VarCharField = VarCharField()
            filling: VarCharField = VarCharField()
            topping: VarCharField = VarCharField()

        statement = (
            Buns
            .select(Buns.name)
            .inner_join(
                fields=[
                    Cookies.filling,
                    Cookies.topping,
                ],
                based_on=Buns.name == Cookies.bun_name,
            )
        )


        async def main() -> None:
            result = await statement
            print(result.as_objects())
            # This will produce this structure list:
            [
                <Buns object>,
                ...
            ]

            bun = result.as_objects()[0]
            # You can get access to the cookies within `bun` object.
            print(bun.cookies.filling)
            # `cookies` - name of the table in join.
            # `filling` - name of the field
        ```
        """
        result_objects: list[FromTable] = []

        for single_query_result in self.query_result:
            zip_expression = zip(
                single_query_result,
                self.aliases.values(),
            )

            temporary_dict: dict[
                type[BaseTable],
                dict[str, Any],
            ] = {}

            for single_query_record, field in zip_expression:
                model_params_dict = temporary_dict.setdefault(
                    field.aliased_field._field_data.from_table,
                    {},
                )
                model_params_dict[
                    field.aliased_field._original_field_name
                ] = single_query_record

            main_table_params = temporary_dict.pop(self.from_table)
            main_table = self.from_table(**main_table_params)
            for model, model_params in temporary_dict.items():
                setattr(
                    main_table,
                    model.original_table_name(),
                    model(**model_params),
                )

            result_objects.append(main_table)

        return result_objects

    def raw_result(self: Self) -> list[tuple[Any, ...]]:
        """Return result of the query as in engine.

        :returns: list of tuples.
        """
        return self.query_result
