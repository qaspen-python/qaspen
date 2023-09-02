import typing
from qaspen.fields.aliases import FieldAliases
from qaspen.table.base_table import BaseTable


class QueryResult:

    def __init__(
        self: typing.Self,
        from_table: type[BaseTable],
        query_result: list[tuple[typing.Any, ...]],
        aliases: FieldAliases,
    ) -> None:
        self.from_table = from_table
        self.query_result = query_result
        self.aliases = aliases

    def as_list(self: typing.Self) -> list[dict[str, typing.Any]]:
        result_list: list[dict[str, typing.Any]] = []

        for single_query_result in self.query_result:
            zip_expression = zip(
                single_query_result,
                self.aliases.values(),
            )

            result_list.append({})

            for single_query_result, field in zip_expression:
                is_from_table: bool = (
                   field.aliased_field._field_data.from_table
                   is self.from_table
                )

                result_dict = result_list[-1]

                if is_from_table:
                    result_dict[
                        field.aliased_field.field_name_clear
                    ] = single_query_result
                else:
                    joined_results = result_dict.setdefault(
                        f"_{field.aliased_field.table_name}",
                        {},
                    )
                    joined_results[
                        field.aliased_field.field_name_clear
                    ] = single_query_result

        return result_list

    def as_objects(self: typing.Self) -> list[BaseTable]:
        result_objects: list[BaseTable] = []

        for single_query_result in self.query_result:
            zip_expression = zip(
                single_query_result,
                self.aliases.values(),
            )

            temporary_dict: dict[
                type[BaseTable], dict[str, typing.Any]
            ] = {}

            for single_query_result, field in zip_expression:
                model_params_dict = temporary_dict.setdefault(
                    field.aliased_field._field_data.from_table,
                    {},
                )
                model_params_dict[
                    field.aliased_field.field_name_clear
                ] = single_query_result

            main_table_params = temporary_dict.pop(self.from_table)
            main_table = self.from_table(**main_table_params)
            for model, model_params in temporary_dict.items():
                setattr(main_table, model._table_name(), model(**model_params))

            result_objects.append(main_table)

        return result_objects
