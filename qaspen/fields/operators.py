class BaseOperator:
    operation_template: str


class GreaterOperator(BaseOperator):
    operation_template: str = "{field_name} > {compare_value}"
