class BaseOperator:
    operation_template: str = ""


class IsNullOperator(BaseOperator):
    operation_template: str = "{} IS NULL"


class IsNotNullOperator(BaseOperator):
    operation_template: str = "{} IS NOT NULL"


class EqualOperator(BaseOperator):
    operation_template: str = "{} = {}"


class NotEqualOperator(BaseOperator):
    operation_template: str = "{} != {}"


class GreaterOperator(BaseOperator):
    operation_template: str = "{} > {}"


class GreaterEqualOperator(BaseOperator):
    operation_template: str = "{} >= {}"


class LessOperator(BaseOperator):
    operation_template: str = "{} < {}"


class LessEqualOperator(BaseOperator):
    operation_template: str = "{} <= {}"


class InOperator(BaseOperator):
    operation_template: str = "{} IN ({})"


class NotInOperator(BaseOperator):
    operation_template: str = "{} NOT IN ({})"


class LikeOperator(BaseOperator):
    operation_template: str = "{} LIKE {}"


class NotLikeOperator(BaseOperator):
    operation_template: str = "{} NOT LIKE {}"


class ILikeOperator(BaseOperator):
    operation_template: str = "{} NOT ILIKE {}"


class NotILikeOperator(BaseOperator):
    operation_template: str = "{} NOT ILIKE {}"


class BetweenOperator(BaseOperator):
    operation_template: str = "{} BETWEEN {} AND {}"


class ANDOperator(BaseOperator):
    operation_template: str = "AND"


class OROperator(BaseOperator):
    operation_template: str = "OR"
