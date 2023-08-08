class BaseOperator:
    operation_template: str = ""


class GreaterOperator(BaseOperator):
    operation_template: str = "{field_name} > {compare_value}"


class ANDOperator(BaseOperator):
    operation_template: str = "AND"


class OROperator(BaseOperator):
    operation_template: str = "OR"
