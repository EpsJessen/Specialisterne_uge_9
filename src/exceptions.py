class TableNotDefinedError(BaseException):
    "Raised when the table seached for is not defined"
    pass

class DictNotFoundError(BaseException):
    "Raised when there is no dictionary at the location"
    pass

class ExtractionError(BaseException):
    "Raise when extraction fails"
    pass

class WrongTablesError(BaseException):
    "The provided tables does not match the tables defined in the task"