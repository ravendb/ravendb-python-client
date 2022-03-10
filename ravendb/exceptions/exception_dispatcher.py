from __future__ import annotations

import http
import os

from ravendb.exceptions.documents import DocumentConflictException
from ravendb.exceptions.raven_exceptions import ConcurrencyException, RavenException


class ExceptionDispatcher:
    class ExceptionSchema:
        def __init__(self, url: str = None, object_type: str = None, message: str = None, error: str = None):
            self.url = url
            self.type = object_type
            self.message = message
            self.error = error

    @staticmethod
    def get(schema: ExceptionDispatcher.ExceptionSchema, code: int, inner: Exception = None) -> RavenException:
        message = schema.message
        type_as_string = schema.type

        if code == http.HTTPStatus.CONFLICT:
            if "DocumentConflictException" in type_as_string:
                return DocumentConflictException.from_message(message)
            return ConcurrencyException(message)

        error = f"{schema.error}{os.linesep}The server at {schema.url} responded with status code: {code}"

        error_type = ExceptionDispatcher.__get_type(type_as_string)
        if error_type is None:
            return RavenException(error, inner)

        try:
            exception = error_type(error)
        except BaseException as e:
            return RavenException(error, inner)

        if not issubclass(error_type, RavenException):
            return RavenException(error, exception)

        return exception

    @staticmethod
    def __get_type(type_as_string: str) -> type:
        if "System.TimeoutException" == type_as_string:
            return TimeoutError

        prefix = "Raven.Client.Exceptions."
        if type_as_string.startswith(prefix):
            exception_name = type_as_string[len(prefix) : :]
            if "." in exception_name:
                exception_name = ".".join(list(map(str.lower, exception_name.split("."))))

            try:
                return __import__(f"pyravendb.exceptions.{exception_name}")  # todo: fix, doesn't work
            except Exception as e:
                return None
        else:
            return None
