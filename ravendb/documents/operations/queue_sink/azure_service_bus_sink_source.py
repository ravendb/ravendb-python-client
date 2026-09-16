from __future__ import annotations


class AzureServiceBusSinkSource:
    """
    Builds the strings that go into QueueSinkScript.queues for an Azure Service Bus sink.

    A queue is its own name; a topic subscription is encoded as "topic;subscription".
    Service Bus naming rules forbid ';' in queue, topic and subscription names, so the
    separator cannot collide with a real name.
    """

    SEPARATOR = ";"

    @staticmethod
    def queue(queue_name: str) -> str:
        """The entry for a Service Bus queue. A pass-through, kept for symmetry with subscription()."""
        if not queue_name or queue_name.isspace():
            raise ValueError("Queue name must be non-empty.")

        if AzureServiceBusSinkSource.SEPARATOR in queue_name:
            raise ValueError(f"Queue name must not contain the '{AzureServiceBusSinkSource.SEPARATOR}' character.")

        return queue_name

    @staticmethod
    def subscription(topic_name: str, subscription_name: str) -> str:
        """The entry for a topic subscription, in the form 'topic;subscription'."""
        if not topic_name or topic_name.isspace():
            raise ValueError("Topic name must be non-empty.")

        if not subscription_name or subscription_name.isspace():
            raise ValueError("Subscription name must be non-empty.")

        separator = AzureServiceBusSinkSource.SEPARATOR
        if separator in topic_name:
            raise ValueError(f"Topic name must not contain the '{separator}' character.")

        if separator in subscription_name:
            raise ValueError(f"Subscription name must not contain the '{separator}' character.")

        return f"{topic_name}{separator}{subscription_name}"
