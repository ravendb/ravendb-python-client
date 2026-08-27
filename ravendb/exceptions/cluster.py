from ravendb.exceptions.raven_exceptions import RavenException


class NoLeaderException(RavenException):
    pass


# The name this class shipped under.
NoLoaderException = NoLeaderException


class NodeIsPassiveException(RavenException):
    pass
