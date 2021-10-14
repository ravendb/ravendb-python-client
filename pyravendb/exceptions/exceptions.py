from __future__ import annotations

import http
from abc import abstractmethod
from typing import Optional

from pyravendb.custom_exceptions.exceptions import RavenException
from pyravendb.exceptions.documents.documents import DocumentConflictException


class ExceptionDispatcher:
    @staticmethod
    def get(schema: ExceptionSchema, code: int, inner: Exception = None) -> RavenException:
        message = schema.message
        type_as_string = schema.type

        if code == http.HTTPStatus.CONFLICT:
            if "DocumentConflictException" in type_as_string:
                return DocumentConflictException.from_message(message)
            return ConcurrencyException(message)

    class ExceptionSchema:
        def __init__(self, url: str, object_type: str, message: str, error: str):
            self.url = url
            self.type = object_type
            self.message = message
            self.error = error


class ConflictException(RavenException):
    @abstractmethod
    def __init__(self, message: str = None, cause: BaseException = None):
        super(ConflictException, self).__init__(message, cause)


class BadResponseException(RavenException):
    def __init__(self, message: str = None, cause: BaseException = None):
        super(BadResponseException, self).__init__(message, cause)


class ClientVersionMismatchException(RavenException):
    def __init__(self, message: Optional[str] = None, cause: Optional[Exception] = None):
        super(ClientVersionMismatchException, self).__init__(message, cause)
