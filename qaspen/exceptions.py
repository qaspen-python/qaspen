class QaspenBaseError(Exception):
    pass


class FieldBaseError(QaspenBaseError):
    pass


class FieldDeclarationError(FieldBaseError):
    pass


class StringFieldDeclarationError(FieldDeclarationError):
    pass
