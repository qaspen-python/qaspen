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


class OnJoinError(QaspenBaseError):
    pass


class OnJoinComparisonError(OnJoinError):
    pass


class OnJoinFieldsError(OnJoinError):
    pass
