class BaseOperator:
    operation_template: str = ""


class IsNullOperator(BaseOperator):
    operation_template: str = "{field_name} IS NULL"


class IsNotNullOperator(BaseOperator):
    operation_template: str = "{field_name} IS NOT NULL"


class EqualOperator(BaseOperator):
    operation_template: str = "{field_name} = {compare_value}"


class NotEqualOperator(BaseOperator):
    operation_template: str = "{field_name} != {compare_value}"


class GreaterOperator(BaseOperator):
    operation_template: str = "{field_name} > {compare_value}"


class GreaterEqualOperator(BaseOperator):
    operation_template: str = "{field_name} >= {compare_value}"


class LessOperator(BaseOperator):
    operation_template: str = "{field_name} < {compare_value}"


class LessEqualOperator(BaseOperator):
    operation_template: str = "{field_name} <= {compare_value}"


class InOperator(BaseOperator):
    operation_template: str = "{field_name} IN ({compare_value})"


class NotInOperator(BaseOperator):
    operation_template: str = "{field_name} NOT IN ({compare_value})"


class BetweenOperator(BaseOperator):
    operation_template: str = (
        "{field_name} BETWEEN {left_comparison_value} "
        "AND {right_comparison_value}"
    )


class ANDOperator(BaseOperator):
    operation_template: str = "AND"


class OROperator(BaseOperator):
    operation_template: str = "OR"
