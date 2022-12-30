from typing import Optional

from ravendb.exceptions.raven_exceptions import RavenException


class BulkInsertAbortedException(RavenException):
    def __init__(self, message: str, cause: Optional[Exception] = None):
        super(BulkInsertAbortedException, self).__init__(message, cause)


class BulkInsertProtocolViolationException(RavenException):
    def __init__(self, message: str, cause: Optional[Exception] = None):
        super(BulkInsertProtocolViolationException, self).__init__(message, cause)
