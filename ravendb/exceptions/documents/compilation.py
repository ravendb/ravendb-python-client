from typing import Optional

from ravendb.exceptions.compilation import CompilationException


class IndexCompilationException(CompilationException):
    def __init__(self, message: str = None, cause: BaseException = None):
        super().__init__(message, cause)
        # Index definition property that caused the error (Maps, Reduce)
        self.index_definition_property: Optional[str] = None
        # Value of the problematic property
        self.problematic_text: Optional[str] = None
