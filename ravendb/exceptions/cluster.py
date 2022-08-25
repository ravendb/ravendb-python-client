from ravendb.exceptions.raven_exceptions import RavenException


class NoLoaderException(RavenException):
    pass


class NodeIsPassiveException(RavenException):
    pass
