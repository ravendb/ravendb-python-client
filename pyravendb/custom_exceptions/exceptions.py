class InvalidOperationException(Exception):
    pass


class NonRecoverableSubscriptionException(Exception):
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


class UnsuccessfulRequestException(Exception):
    pass


class AggregateException(Exception):
    pass


class NotSupportedException(Exception):
    pass


class AggregateException(Exception):
    pass


class ChangeProcessingException(Exception):
    pass


class ChangeProcessingException(Exception):
    pass


# <---------- Subscription Exceptions ---------->

class SubscriptionException(Exception):
    pass


class SubscriptionInUseException(SubscriptionException):
    pass


class SubscriptionClosedException(SubscriptionException):
    pass


class SubscriptionInvalidStateException(SubscriptionException):
    pass


class SubscriptionDoesNotExistException(SubscriptionException):
    pass


class SubscriptionDoesNotBelongToNodeException(SubscriptionException):
    def __init__(self, appropriate_node, message):
        super(SubscriptionDoesNotBelongToNodeException, self).__init__(message)
        self.appropriate_node = appropriate_node


class SubscriptionChangeVectorUpdateConcurrencyException(SubscriptionException):
    pass


class SubscriberErrorException(Exception):
    pass
