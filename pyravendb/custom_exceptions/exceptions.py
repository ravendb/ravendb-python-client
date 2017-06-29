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


class DatabaseDoesNotExistException(Exception):
    pass


class AuthorizationException(Exception):
    pass


class IndexDoesNotExistException(Exception):
    pass


class TimeoutException(Exception):
    pass


class AuthenticationException(Exception):
    pass

class AllTopologyNodesDownException(Exception):
    pass
