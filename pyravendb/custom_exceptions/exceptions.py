class InvalidOperationException(Exception):
    pass


class ErrorResponseException(Exception):
    pass


class DocumentDoesNotExistsException(Exception):
    pass


class NonUniqueObjectException(Exception):
    pass


class FetchConcurrencyException(Exception):
    pass


class ArgumentOutOfRangeException(Exception):
    pass


class TimeoutException(Exception):
    pass


class NotSupportedException(Exception):
    pass