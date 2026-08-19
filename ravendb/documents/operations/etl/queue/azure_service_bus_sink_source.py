from __future__ import annotations

from typing import List, Optional, Tuple


class AzureServiceBusSinkSource:
    """Helpers for encoding Azure Service Bus sources as strings.

    Encoding convention: a plain queue name is a Service Bus queue, and
    "topic;subscription" is a topic subscription. The ';' separator is
    collision-safe because Service Bus naming rules forbid it in names.
    """

    SEPARATOR = ";"

    @staticmethod
    def queue(queue_name: str) -> str:
        if not queue_name or (isinstance(queue_name, str) and queue_name.isspace()):
            raise ValueError("Queue name must be non-empty.")
        if AzureServiceBusSinkSource.SEPARATOR in queue_name:
            raise ValueError("Queue name must not contain the ';' character.")
        return queue_name

    @staticmethod
    def subscription(topic_name: str, subscription_name: str) -> str:
        if not topic_name or (isinstance(topic_name, str) and topic_name.isspace()):
            raise ValueError("Topic name must be non-empty.")
        if not subscription_name or (isinstance(subscription_name, str) and subscription_name.isspace()):
            raise ValueError("Subscription name must be non-empty.")
        if AzureServiceBusSinkSource.SEPARATOR in topic_name:
            raise ValueError("Topic name must not contain the ';' character.")
        if AzureServiceBusSinkSource.SEPARATOR in subscription_name:
            raise ValueError("Subscription name must not contain the ';' character.")
        return f"{topic_name}{AzureServiceBusSinkSource.SEPARATOR}{subscription_name}"

    @staticmethod
    def validate_entry(entry: str) -> Optional[str]:
        """Returns None when the entry is a valid queue name or a valid
        topic;subscription pair, otherwise the error message."""
        if not entry or (isinstance(entry, str) and entry.isspace()):
            return "Azure Service Bus source entry cannot be empty."

        if AzureServiceBusSinkSource.SEPARATOR in entry and not AzureServiceBusSinkSource.try_parse_subscription(entry):
            return (
                f"Azure Service Bus subscription source '{entry}' is invalid. "
                f"Use '<topic>{AzureServiceBusSinkSource.SEPARATOR}<subscription>' with a single "
                f"'{AzureServiceBusSinkSource.SEPARATOR}' separator and both parts non-empty."
            )

        return None

    @staticmethod
    def validate_script(script_name: str, queues: Optional[List[str]]) -> List[str]:
        errors = []
        if queues is None:
            return errors

        for entry in queues:
            error = AzureServiceBusSinkSource.validate_entry(entry)
            if error is None:
                continue
            errors.append(f"Script '{script_name}': {error}")

        return errors

    @staticmethod
    def try_parse_subscription(entry: str) -> Optional[Tuple[str, str]]:
        if entry is None:
            return None

        parts = entry.split(AzureServiceBusSinkSource.SEPARATOR)
        if len(parts) != 2:
            return None

        if not parts[0] or parts[0].isspace() or not parts[1] or parts[1].isspace():
            return None

        return parts[0], parts[1]
