from __future__ import annotations

import http
import os
from datetime import timedelta

from ravendb.exceptions.cluster import NodeIsPassiveException, NoLoaderException
from ravendb.exceptions.documents import DocumentConflictException, DocumentDoesNotExistException
from ravendb.exceptions.documents.bulkinsert import BulkInsertAbortedException, BulkInsertProtocolViolationException
from ravendb.exceptions.documents.indexes import IndexDoesNotExistException
from ravendb.exceptions.raven_exceptions import (
    AiException,
    BadResponseException,
    ClientVersionMismatchException,
    ConcurrencyException,
    IndexCompactionInProgressException,
    InsufficientQuotaException,
    MissingAiAgentParameterException,
    PortInUseException,
    RateLimitException,
    RavenException,
    RefusedToAnswerException,
    ReplicationHubNotFoundException,
    SchemaValidationException,
    TooManyRequestsException,
    TooManyTokensException,
    UnsuccessfulAiRequestException,
)

# Maps the simple C# class name to the Python exception class.
# C# type strings look like "Raven.Client.Exceptions.Documents.DocumentConflictException"
# — we match on the last segment only.
_EXCEPTION_MAP: dict = {
    # raven_exceptions.py
    "RavenException": RavenException,
    "BadResponseException": BadResponseException,
    "ConcurrencyException": ConcurrencyException,
    "ClientVersionMismatchException": ClientVersionMismatchException,
    "PortInUseException": PortInUseException,
    "IndexCompactionInProgressException": IndexCompactionInProgressException,
    # AI exceptions
    "AiException": AiException,
    "RefusedToAnswerException": RefusedToAnswerException,
    "UnsuccessfulAiRequestException": UnsuccessfulAiRequestException,
    "TooManyRequestsException": TooManyRequestsException,
    "RateLimitException": RateLimitException,
    "InsufficientQuotaException": InsufficientQuotaException,
    "TooManyTokensException": TooManyTokensException,
    "MissingAiAgentParameterException": MissingAiAgentParameterException,
    # documents
    "DocumentConflictException": DocumentConflictException,
    "DocumentDoesNotExistException": DocumentDoesNotExistException,
    "IndexDoesNotExistException": IndexDoesNotExistException,
    "BulkInsertAbortedException": BulkInsertAbortedException,
    "BulkInsertProtocolViolationException": BulkInsertProtocolViolationException,
    # schema validation
    "SchemaValidationException": SchemaValidationException,
    # replication
    "ReplicationHubNotFoundException": ReplicationHubNotFoundException,
    # cluster
    "NodeIsPassiveException": NodeIsPassiveException,
    "NoLoaderException": NoLoaderException,
}


class ExceptionDispatcher:
    class ExceptionSchema:
        def __init__(self, url: str = None, object_type: str = None, message: str = None, error: str = None):
            self.url = url
            self.type = object_type
            self.message = message
            self.error = error

    @staticmethod
    def get(
        schema: ExceptionDispatcher.ExceptionSchema, code: int, inner: Exception = None, json_body: dict = None
    ) -> RavenException:
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

        if json_body:
            ExceptionDispatcher.__fill_exception(exception, json_body)

        return exception

    @staticmethod
    def __fill_exception(exception: RavenException, data: dict) -> None:
        if isinstance(exception, RateLimitException):
            # StatusCode is always 429 for RateLimitException — set it directly (mirrors C# FillException)
            exception.status_code = 429
            retry_after_raw = data.get("RetryAfter")
            if retry_after_raw:
                try:
                    # RetryAfter is a C# TimeSpan serialized as "hh:mm:ss[.fffffff]"
                    parts = retry_after_raw.split(":")
                    if len(parts) == 3:
                        h, m, s = int(parts[0]), int(parts[1]), float(parts[2])
                        exception.retry_after = timedelta(hours=h, minutes=m, seconds=s)
                    else:
                        exception.retry_after = timedelta(seconds=float(retry_after_raw))
                except Exception:
                    pass
        elif isinstance(exception, UnsuccessfulAiRequestException):
            status_code = data.get("StatusCode")
            if status_code is not None:
                try:
                    exception.status_code = int(status_code)
                except Exception:
                    pass
        elif isinstance(exception, RefusedToAnswerException):
            exception.refusal = data.get("Refusal")
            exception.finish_reason = data.get("FinishReason")

        if isinstance(exception, AiException):
            exception.request_id = data.get("RequestId")

    @staticmethod
    def __get_type(type_as_string: str) -> type:
        if type_as_string == "System.TimeoutException":
            return TimeoutError

        # C# type strings: "Raven.Client.Exceptions[.Namespace].ClassName"
        # Match on the simple class name regardless of namespace depth.
        simple_name = type_as_string.split(".")[-1]
        return _EXCEPTION_MAP.get(simple_name)
