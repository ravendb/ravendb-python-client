from typing import Optional

from pyravendb.custom_exceptions.exceptions import RavenException


class ClientVersionMismatchException(RavenException):
    def __init__(self, message: Optional[str] = None, cause: Optional[Exception] = None):
        super(ClientVersionMismatchException, self).__init__(message, cause)
