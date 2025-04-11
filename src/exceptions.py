class TableNotDefinedError(Exception):
    "Raised when the table seached for is not defined"
    pass

class DictNotFoundError(Exception):
    "Raised when there is no dictionary at the location"
    pass

class ExtractionError(Exception):
    "Raise when extraction fails"
    pass

class WrongTablesError(Exception):
    "The provided tables does not match the tables defined in the task"