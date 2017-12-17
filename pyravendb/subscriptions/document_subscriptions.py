from pyravendb.custom_exceptions.exceptions import InvalidOperationException
from pyravendb.commands.raven_commands import CreateSubscriptionCommand, DeleteSubscriptionCommand, \
    GetSubscriptionsCommand, GetSubscriptionStateCommand, DropSubscriptionConnectionCommand
from threading import Lock
from pyravendb.subscriptions.subscription import SubscriptionWorker
from pyravendb.subscriptions.data import *


class DocumentSubscriptions:
    def __init__(self, store):
        self._store = store
        self.lock = Lock()
        self._subscriptions = set()

    def create(self, options_or_query, database=None):
        """
        @param str or SubscriptionCreationOptions options_or_query: The query or the options to create the subscription
        @param str database: The database name
        :returns: returns The subscription name
        """
        options = options_or_query
        if isinstance(options_or_query, str):
            options = SubscriptionCreationOptions(query=options_or_query)

        return self._create(options, database)

    def _create(self, options, database=None):
        if options is None:
            raise InvalidOperationException("Cannot create a subscription if the options is set to null")
        if not isinstance(options, SubscriptionCreationOptions):
            raise InvalidOperationException("options must be SubscriptionCreationOptions")
        if options.query is None:
            raise InvalidOperationException("Cannot create a subscription if the script is null")

        request_executor = self._store.get_request_executor(database)
        command = CreateSubscriptionCommand(options)
        return request_executor.execute(command)

    def get_subscription_worker(self, options, database=None, object_type=None, nested_object_types=None):
        """
        @param SubscriptionWorkerOptions options: The subscription connection options.
        @param str database: The database name
        @param Type object_type: The type of the object we want to track the entity to
        @param Dict[str, Type] nested_object_types: A dict of classes for nested object the key will be the name of the
        class and the value will be the object we want to get for that attribute
        """
        if options is None:
            raise InvalidOperationException("Cannot create a subscription if the options is set to None")
        if not isinstance(options, SubscriptionWorkerOptions):
            raise ValueError("Invalid options", "options mus be SubscriptionWorkerOptions")

        subscription = SubscriptionWorker(options, self._store, database, object_type=object_type,
                                          nested_object_types=nested_object_types)
        with self.lock:
            self._subscriptions.add(subscription)

        return subscription

    def delete(self, name_or_id, database=None):
        request_executor = self._store.get_request_executor(database)
        command = DeleteSubscriptionCommand(name_or_id)
        request_executor.execute(command)

    def drop_connection(self, name, database=None):
        """
        Force server to close current client subscription connection to the server
        @param str name: The name of the subscription
        @param str database: The name of the database
        """
        request_executor = self._store.get_request_executor(database)
        command = DropSubscriptionConnectionCommand(name)
        request_executor.execute(command)

    def get_subscriptions(self, start, take, database=None):
        """
        @param int start: Where to start and take the subscriptions
        @param int take: How much subscription to get
        @param str database: The database name
        @returns: returns a list of SubscriptionState
        """
        request_executor = self._store.get_request_executor(database)
        command = GetSubscriptionsCommand(start, take)
        return request_executor.execute(command)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if len(self._subscriptions) == 0:
            return

        for subscription in self._subscriptions:
            subscription.close()

    def get_subscription_state(self, subscription_name, database=None):
        request_executor = self._store.get_request_executor(database)
        command = GetSubscriptionStateCommand(subscription_name)
        return request_executor.execute(command)
