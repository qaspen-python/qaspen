class BaseOperator:
    operation_template: str = ""


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


class ANDOperator(BaseOperator):
    operation_template: str = "AND"


class OROperator(BaseOperator):
    operation_template: str = "OR"
