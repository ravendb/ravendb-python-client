from __future__ import annotations
import datetime
from typing import Optional, Dict

from ravendb.tools.utils import Utils


class SubscriptionState:
    def __init__(
        self,
        query: Optional[str] = None,
        change_vector_for_next_batch_starting_point: Optional[str] = None,
        subscription_id: Optional[int] = None,
        subscription_name: Optional[str] = None,
        mentor_node: Optional[str] = None,
        node_tag: Optional[str] = None,
        last_batch_ack_time: Optional[datetime.datetime] = None,
        last_client_connection_time: Optional[datetime.datetime] = None,
        disabled: Optional[bool] = None,
    ):
        self.query = query
        self.change_vector_for_next_batch_starting_point = change_vector_for_next_batch_starting_point
        self.subscription_id = subscription_id
        self.subscription_name = subscription_name
        self.mentor_node = mentor_node
        self.node_tag = node_tag
        self.last_batch_ack_time = last_batch_ack_time
        self.last_client_connection_time = last_client_connection_time
        self.disabled = disabled

    @classmethod
    def from_json(cls, json_dict: Dict) -> SubscriptionState:
        return cls(
            json_dict["Query"],
            json_dict["ChangeVectorForNextBatchStartingPoint"],
            json_dict["SubscriptionId"],
            json_dict["SubscriptionName"],
            json_dict.get("MentorNode", None),
            json_dict.get("NodeTag", None),
            Utils.string_to_datetime(json_dict["LastBatchAckTime"]),
            Utils.string_to_datetime(json_dict["LastClientConnectionTime"]),
            json_dict["Disabled"],
        )
