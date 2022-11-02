from datetime import timedelta
from enum import Enum
from typing import Callable, Optional, Dict

from ravendb.documents.session.loaders.include import SubscriptionIncludeBuilder
from ravendb.tools.utils import Utils


class SubscriptionOpeningStrategy(Enum):
    OPEN_IF_FREE = "OpenIfFree"
    TAKE_OVER = "TakeOver"
    WAIT_FOR_FREE = "WaitForFree"


class SubscriptionCreationOptions:
    def __init__(
        self,
        name: Optional[str] = None,
        query: Optional[str] = None,
        includes: Optional[Callable[[SubscriptionIncludeBuilder], None]] = None,
        change_vector: Optional[str] = None,
        mentor_node: Optional[str] = None,
    ):
        self.name = name
        self.query = query
        self.includes = includes
        self.change_vector = change_vector
        self.mentor_node = mentor_node

    def to_json(self) -> Dict:
        return {
            "Name": self.name,
            "Query": self.query,
            "Includes": dict(),
            "ChangeVector": self.change_vector,
            "MentorNode": self.mentor_node,
        }


class SubscriptionWorkerOptions:
    def __init__(
        self,
        subscription_name: str,
        strategy: SubscriptionOpeningStrategy = SubscriptionOpeningStrategy.OPEN_IF_FREE,
        max_docs_per_batch: int = 4096,
        time_to_wait_before_connection_retry: timedelta = timedelta(seconds=5),
        max_erroneous_period: timedelta = timedelta(minutes=5),
        receive_buffer_size: int = 32 * 1024,
        send_buffer_size: int = 32 * 1024,
        ignore_subscriber_errors: Optional[bool] = None,
        close_when_no_docs_left: Optional[bool] = None,
    ):
        if not subscription_name or subscription_name.isspace():
            raise ValueError("Subscription name cannot be None or empty")
        self.subscription_name = subscription_name
        self.strategy = strategy
        self.max_docs_per_batch = max_docs_per_batch
        self.time_to_wait_before_connection_retry = time_to_wait_before_connection_retry
        self.max_erroneous_period = max_erroneous_period
        self.receive_buffer_size = receive_buffer_size
        self.send_buffer_size = send_buffer_size
        self.ignore_subscriber_errors = ignore_subscriber_errors
        self.close_when_no_docs_left = close_when_no_docs_left

    def to_json(self) -> Dict:
        return {
            "SubscriptionName": self.subscription_name,
            "Strategy": self.strategy.value,
            "MaxDocsPerBatch": self.max_docs_per_batch,
            "TimeToWaitBeforeConnectionRetry": Utils.timedelta_to_str(self.time_to_wait_before_connection_retry),
            "MaxErroneousPeriod": Utils.timedelta_to_str(self.max_erroneous_period),
            "ReceiveBufferSize": self.receive_buffer_size,
            "SendBufferSize": self.send_buffer_size,
            "IgnoreSubscriberErrors": self.ignore_subscriber_errors,
            "CloseWhenNoDocsLeft": self.close_when_no_docs_left,
        }


class SubscriptionUpdateOptions(SubscriptionCreationOptions):
    def __init__(
        self,
        name: Optional[str] = None,
        query: Optional[str] = None,
        includes: Optional[Callable[[SubscriptionIncludeBuilder], None]] = None,
        change_vector: Optional[str] = None,
        mentor_node: Optional[str] = None,
        key: Optional[int] = None,
        create_new: Optional[bool] = None,
    ):
        super(SubscriptionUpdateOptions, self).__init__(name, query, includes, change_vector, mentor_node)
        self.key = key
        self.create_new = create_new

    def to_json(self) -> Dict:
        json_dict = super().to_json()
        json_dict.update({"Id": self.key, "CreateNew": self.create_new})
        return json_dict
