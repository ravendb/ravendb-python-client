from abc import abstractmethod
from typing import Optional


class RavenException(RuntimeError):
    def __init__(self, message: str = None, cause: BaseException = None):
        super(RavenException, self).__init__(*(message, cause) if cause else message)
        self.reached_leader = None

    @staticmethod
    def generic(error: str, json: str):
        return RavenException(f"{error}. Response: {json}")


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