import typing


class QueryString:
    add_delimiter: str = " "

    def __init__(
        self: typing.Self,
        *template_arguments: typing.Any,
        sql_template: str,
    ) -> None:
        self.sql_template: str = sql_template
        self.template_arguments: list[typing.Any] = list(template_arguments)

    @classmethod
    def empty(cls: type["QueryString"]) -> "EmptyQueryString":
        return EmptyQueryString(sql_template="")

    def querystring(self: typing.Self) -> str:
        return self.sql_template.format(
            *self.template_arguments,
        )

    def __add__(
        self: typing.Self,
        additional_querystring: "QueryString",
    ) -> typing.Self:
        if isinstance(additional_querystring, EmptyQueryString):
            return self

        self.sql_template += (
            f"{self.add_delimiter}" f"{additional_querystring.sql_template}"
        )
        self.template_arguments.extend(
            additional_querystring.template_arguments,
        )
        return self

    def __str__(self: typing.Self) -> str:
        return self.querystring()


class EmptyQueryString(QueryString):
    add_delimiter: str = ""


class OrderByQueryString(QueryString):
    add_delimiter: str = ", "


class WhereQueryString(QueryString):
    add_delimiter: str = " AND "
