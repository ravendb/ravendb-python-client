import os
from typing import Optional, Type, TypeVar, Dict, List, TYPE_CHECKING

from ravendb.documents.operations.ongoing_tasks import ToggleOngoingTaskStateOperation, OngoingTaskType
from ravendb.documents.session.utils.includes_util import IncludesUtil
from ravendb.documents.commands.subscriptions import (
    CreateSubscriptionCommand,
    GetSubscriptionsCommand,
    DeleteSubscriptionCommand,
    GetSubscriptionStateCommand,
    DropSubscriptionConnectionCommand,
    UpdateSubscriptionCommand,
)
from ravendb.documents.session.loaders.include import SubscriptionIncludeBuilder
from ravendb.documents.subscriptions.options import (
    SubscriptionCreationOptions,
    SubscriptionWorkerOptions,
    SubscriptionUpdateOptions,
)
from ravendb.documents.subscriptions.revision import Revision
from ravendb.documents.subscriptions.state import SubscriptionState
from ravendb.documents.subscriptions.worker import SubscriptionWorker
from ravendb.extensions.string_extensions import escape_string

_T = TypeVar("_T")

if TYPE_CHECKING:
    from ravendb.documents.store.definition import DocumentStore


class DocumentSubscriptions:
    def __init__(self, store: "DocumentStore"):
        self._store = store
        self._subscriptions: Dict[SubscriptionWorker, bool] = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def create_for_options(self, options: SubscriptionCreationOptions, database: Optional[str] = None) -> str:
        if options is None:
            raise ValueError("Cannot create a subscription if options is None")

        if options.query is None:
            raise ValueError("Cannot create a subscriptions if the script is None")

        request_executor = self._store.get_request_executor(self._store.get_effective_database(database))

        command = CreateSubscriptionCommand(self._store.conventions, options)
        request_executor.execute_command(command)

        return command.result.name

    def create_for_options_autocomplete_query(
        self,
        object_type: Type[_T],
        options: SubscriptionCreationOptions = SubscriptionCreationOptions(),
        database: Optional[str] = None,
    ) -> str:
        options = self.ensure_criteria(options, object_type, False)
        return self.create_for_options(options, database)

    def create_for_class(
        self,
        object_type: Type[_T],
        options: Optional[SubscriptionCreationOptions] = None,
        database: Optional[str] = None,
    ) -> str:
        options = options or SubscriptionCreationOptions()
        return self.create_for_options(self.ensure_criteria(options, object_type, False), database)

    def ensure_criteria(self, criteria: SubscriptionCreationOptions, object_type: Type[_T], revisions: bool):
        if criteria is None:
            criteria = SubscriptionCreationOptions()

        collection_name = self._store.conventions.get_collection_name(object_type)

        if criteria.query:
            query_builder = [criteria.query]
        else:
            query_builder = ["from '"]
            escape_string(query_builder, collection_name)
            query_builder.append("'")

            if revisions:
                query_builder.append(" (Revisions = true)")

            query_builder.append(" as doc")

        if criteria.includes:
            builder = SubscriptionIncludeBuilder(self._store.conventions)
            criteria.includes(builder)

            number_of_includes_added = 0

            if builder._documents_to_include is not None and not len(builder._documents_to_include) == 0:
                query_builder.append(os.linesep)
                query_builder.append("include ")

                for inc in builder._documents_to_include:
                    include = "doc." + inc
                    if number_of_includes_added > 0:
                        query_builder.append(",")

                    escaped_include = None
                    req, escaped_include = IncludesUtil.requires_quotes(include)
                    if req:
                        query_builder.append("'")
                        query_builder.append(escaped_include)
                        query_builder.append("'")
                    else:
                        query_builder.append(include)

                    number_of_includes_added += 1
            # todo: uncomment on Counters and TimeSeries development
            # if builder._is_all_counters:
            #     if number_of_includes_added == 0:
            #         query_builder.append(os.linesep)
            #         query_builder.append("include ")
            #
            #     token = CountersIncludesToken.all("")
            #     token.write_to(query_builder)
            #     number_of_includes_added += 1
            #
            # elif builder._counters_to_include:
            #     if number_of_includes_added:
            #         query_builder.append(os.linesep)
            #         query_builder.append("include ")
            #
            #     for counter_name in builder._counters_to_include:
            #         if number_of_includes_added > 0:
            #             query_builder.append(",")
            #
            #         token = CountersToIncludeToken.create("", counter_name)
            #         token.write_to(query_builder)
            #
            #         number_of_includes_added += 1
            #
            # if builder._time_series_to_include:
            #     for time_series_range in builder._time_series_to_include:
            #         if number_of_includes_added == 0:
            #             query_builder.append(os.linesep)
            #             query_builder.append("include ")
            #
            #         if number_of_includes_added > 0:
            #             query_builder.append(",")
            #
            #         token = TimeSeriesIncludeToken.create("", time_series_range)
            #         token.write_to(query_builder)

        criteria.query = "".join(query_builder)
        return criteria

    def get_subscription_worker(
        self, options: SubscriptionWorkerOptions, object_type: Optional[Type[_T]] = None, database: Optional[str] = None
    ) -> SubscriptionWorker[_T]:
        self._store.assert_initialized()
        if options is None:
            raise RuntimeError("Cannot open a subscription if options are None")

        subscription = SubscriptionWorker(object_type, options, False, self._store, database)

        subscription._on_closed = lambda sender: self._subscriptions.pop(sender)
        self._subscriptions[subscription] = True

        return subscription

    def get_subscription_worker_by_name(
        self,
        subscription_name: Optional[str] = None,
        object_type: Optional[Type[_T]] = None,
        database: Optional[str] = None,
    ) -> SubscriptionWorker[_T]:
        return self.get_subscription_worker(SubscriptionWorkerOptions(subscription_name), object_type, database)

    def get_subscription_worker_for_revisions(
        self, options: SubscriptionWorkerOptions, object_type: Optional[Type[_T]] = None, database: Optional[str] = None
    ) -> SubscriptionWorker[Revision[_T]]:
        subscription = SubscriptionWorker(object_type, options, True, self._store, database)

        subscription._on_closed = lambda sender: self._subscriptions.pop(sender)
        self._subscriptions[subscription] = True

        return subscription

    def get_subscriptions(self, start: int, take: int, database: Optional[str] = None) -> List[SubscriptionState]:
        request_executor = self._store.get_request_executor(self._store.get_effective_database(database))

        command = GetSubscriptionsCommand(start, take)
        request_executor.execute_command(command)

        return command.result

    def delete(self, name: str, database: Optional[str] = None) -> None:
        request_executor = self._store.get_request_executor(self._store.get_effective_database(database))

        command = DeleteSubscriptionCommand(name)
        request_executor.execute_command(command)

    def get_subscription_state(self, subscription_name: str, database: Optional[str] = None) -> SubscriptionState:
        if not subscription_name or subscription_name.isspace():
            raise ValueError("Subscription name cannot be None")

        request_executor = self._store.get_request_executor(self._store.get_effective_database(database))

        command = GetSubscriptionStateCommand(subscription_name)
        request_executor.execute_command(command)
        return command.result

    def close(self) -> None:
        if not self._subscriptions:
            return

        for subscription in self._subscriptions:
            subscription.close()

    def drop_connection(self, name: str, database: Optional[str] = None) -> None:
        request_executor = self._store.get_request_executor(self._store.get_effective_database(database))

        command = DropSubscriptionConnectionCommand(name)
        request_executor.execute_command(command)

    def enable(self, name: str, database: Optional[str] = None) -> None:
        operation = ToggleOngoingTaskStateOperation(name, OngoingTaskType.SUBSCRIPTION, False)
        self._store.maintenance.for_database(self._store.get_effective_database(database)).send(operation)

    def disable(self, name: str, database: Optional[str] = None) -> None:
        operation = ToggleOngoingTaskStateOperation(name, OngoingTaskType.SUBSCRIPTION, True)
        self._store.maintenance.for_database(self._store.get_effective_database(database)).send(operation)

    def update(self, options: SubscriptionUpdateOptions, database: Optional[str] = None) -> str:
        if options is None:
            raise ValueError("Cannot update a subscription if options is None")

        if not options.name and options.key is None:
            raise ValueError("Cannot update a subscription if both options.name nad options.id are None")

        request_executor = self._store.get_request_executor(database)
        command = UpdateSubscriptionCommand(options)
        request_executor.execute_command(command, None)

        return command.result.name
