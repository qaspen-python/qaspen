class QaspenBaseError(Exception):
    pass


class FieldBaseError(QaspenBaseError):
    pass


class FieldDeclarationError(FieldBaseError):
    pass


class StringFieldDeclarationError(FieldDeclarationError):
    pass


class FieldComparisonError(FieldBaseError):
    pass


class WhereComparisonError(FieldBaseError):
    pass
