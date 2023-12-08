from qaspen.querystring.querystring import QueryString


class BaseOperator:
    operation_template: str = ""


class IsNullOperator(BaseOperator):
    operation_template: str = f"{QueryString.arg_ph()} IS NULL"


class IsNotNullOperator(BaseOperator):
    operation_template: str = f"{QueryString.arg_ph()} IS NOT NULL"


class EqualOperator(BaseOperator):
    operation_template: str = (
        f"{QueryString.arg_ph()} = {QueryString.param_ph()}"
    )


class NotEqualOperator(BaseOperator):
    operation_template: str = (
        f"{QueryString.arg_ph()} != {QueryString.param_ph()}"
    )


class GreaterOperator(BaseOperator):
    operation_template: str = (
        f"{QueryString.arg_ph()} > {QueryString.param_ph()}"
    )


class GreaterEqualOperator(BaseOperator):
    operation_template: str = (
        f"{QueryString.arg_ph()} >= {QueryString.param_ph()}"
    )


class LessOperator(BaseOperator):
    operation_template: str = (
        f"{QueryString.arg_ph()} < {QueryString.param_ph()}"
    )


class LessEqualOperator(BaseOperator):
    operation_template: str = (
        f"{QueryString.arg_ph()} <= {QueryString.param_ph()}"
    )


class InOperator(BaseOperator):
    operation_template: str = (
        f"{QueryString.arg_ph()} IN ({QueryString.param_ph()})"
    )


class NotInOperator(BaseOperator):
    operation_template: str = (
        f"{QueryString.arg_ph()} NOT IN ({QueryString.param_ph()})"
    )


class LikeOperator(BaseOperator):
    operation_template: str = (
        f"{QueryString.arg_ph()} LIKE {QueryString.param_ph()}"
    )


class NotLikeOperator(BaseOperator):
    operation_template: str = (
        f"{QueryString.arg_ph()} NOT LIKE {QueryString.param_ph()}"
    )


class ILikeOperator(BaseOperator):
    operation_template: str = (
        f"{QueryString.arg_ph()} ILIKE {QueryString.param_ph()}"
    )


class NotILikeOperator(BaseOperator):
    operation_template: str = (
        f"{QueryString.arg_ph()} NOT ILIKE {QueryString.param_ph()}"
    )


class BetweenOperator(BaseOperator):
    operation_template: str = (
        f"{QueryString.arg_ph()} BETWEEN "
        f"{QueryString.param_ph()} AND {QueryString.param_ph()}"
    )


class ANDOperator(BaseOperator):
    operation_template: str = "AND"


class OROperator(BaseOperator):
    operation_template: str = "OR"


class NotOperator(BaseOperator):
    operation_template: str = f"NOT ({QueryString.param_ph()})"
