from abc import abstractmethod
from datetime import timedelta
from enum import Enum
from typing import Optional


class RavenException(RuntimeError):
    def __init__(self, message: str = None, cause: BaseException = None):
        super(RavenException, self).__init__(message)
        self.cause = cause
        self.reached_leader = None

    @classmethod
    def generic(cls, error: str, json: str):
        return cls(f"{error}. Response: {json}")


class BadResponseException(RavenException):
    def __init__(self, message: str = None, cause: BaseException = None):
        super(BadResponseException, self).__init__(message, cause)


class ConflictException(RavenException):
    @abstractmethod
    def __init__(self, message: str = None, cause: BaseException = None):
        super().__init__(message, cause)


class ConcurrencyException(ConflictException):
    def __init__(self, message):
        super().__init__(message)


class ClientVersionMismatchException(RavenException):
    def __init__(self, message: Optional[str] = None, cause: Optional[Exception] = None):
        super().__init__(message, cause)


class PortInUseException(RavenException):
    pass


class IndexCompactionInProgressException(RavenException):
    pass


class AiException(RavenException):
    def __init__(self, message: str = None, cause: BaseException = None):
        super().__init__(message, cause)
        self.request_id: Optional[str] = None


class RefusedToAnswerException(AiException):
    def __init__(self, message: str = None):
        super().__init__(message)
        self.refusal: Optional[str] = None
        self.finish_reason: Optional[str] = None


class UnsuccessfulAiRequestException(AiException):
    def __init__(self, message: str = None, status_code: int = 0):
        super().__init__(message)
        self.status_code = status_code


class TooManyRequestsException(UnsuccessfulAiRequestException):
    def __init__(self, message: str = None):
        super().__init__(message, status_code=429)


class RateLimitException(TooManyRequestsException):
    def __init__(self, message: str = None):
        super().__init__(message)
        self.retry_after: Optional[timedelta] = None


class InsufficientQuotaException(TooManyRequestsException):
    pass


class TooManyTokensException(TooManyRequestsException):
    pass


class MissingAiAgentParameterException(RavenException):
    pass


class SchemaValidationException(RavenException):
    def __init__(self, message: str = None):
        super().__init__(message)


class ReplicationHubNotFoundException(RavenException):
    def __init__(self, message: str = None, cause: BaseException = None):
        super().__init__(message, cause)
