"""Tests for the Azure Service Bus surface: the broker type,
the connection settings classes with exactly-one-auth validation, the queue
connection-string field, and the sink-source helpers with entry validation.
"""

import unittest

from ravendb.documents.operations.etl.queue.azure_service_bus_connection_settings import (
    AzureServiceBusConnectionSettings,
    AzureServiceBusEntraId,
    AzureServiceBusPasswordless,
)
from ravendb.documents.operations.etl.queue.azure_service_bus_sink_source import AzureServiceBusSinkSource
from ravendb.documents.operations.etl.queue.connection import QueueBrokerType, QueueConnectionString
from ravendb.exceptions.exceptions import InvalidOperationException


class TestQueueBrokerType(unittest.TestCase):
    def test_azure_service_bus_value(self):
        self.assertEqual("AzureServiceBus", QueueBrokerType.AZURE_SERVICE_BUS.value)


class TestAzureServiceBusConnectionSettings(unittest.TestCase):
    def test_connection_string_valid_when_contains_sb_protocol(self):
        settings = AzureServiceBusConnectionSettings(
            connection_string="Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=v"
        )
        self.assertTrue(settings.is_valid_connection())

    def test_sb_search_is_case_insensitive(self):
        settings = AzureServiceBusConnectionSettings(connection_string="SB://ns.servicebus.windows.net")
        self.assertTrue(settings.is_valid_connection())

    def test_connection_string_invalid_without_sb_protocol(self):
        for value in (
            "SharedAccessKeyName=key;SharedAccessKey=abc",
            "Endpoint=https://ns.servicebus.windows.net/;SharedAccessKey=abc",
            "nothing-useful-here",
        ):
            self.assertFalse(AzureServiceBusConnectionSettings(connection_string=value).is_valid_connection())

    def test_whitespace_connection_string_counts_as_unset(self):
        settings = AzureServiceBusConnectionSettings(connection_string="   ")
        self.assertFalse(settings.is_valid_connection())
        # ToJson asymmetry: IsNullOrEmpty writes a whitespace-only value.
        self.assertEqual({"ConnectionString": "   "}, settings.to_json())

    def test_exactly_one_auth_method_required(self):
        none_set = AzureServiceBusConnectionSettings()
        self.assertFalse(none_set.is_valid_connection())

        two_set = AzureServiceBusConnectionSettings(
            connection_string="Endpoint=sb://x/",
            entra_id=AzureServiceBusEntraId(namespace="ns", tenant_id="t", client_id="c", client_secret="s"),
        )
        self.assertFalse(two_set.is_valid_connection())

    def test_entra_id_requires_all_four_fields(self):
        complete = AzureServiceBusEntraId(namespace="ns", tenant_id="t", client_id="c", client_secret="s")
        self.assertTrue(AzureServiceBusConnectionSettings(entra_id=complete).is_valid_connection())
        for kwargs in (
            {"namespace": "ns", "tenant_id": "t", "client_id": "c"},
            {"namespace": "ns", "tenant_id": "t", "client_id": "c", "client_secret": "  "},
        ):
            self.assertFalse(
                AzureServiceBusConnectionSettings(entra_id=AzureServiceBusEntraId(**kwargs)).is_valid_connection()
            )

    def test_passwordless_requires_namespace(self):
        self.assertTrue(
            AzureServiceBusConnectionSettings(
                passwordless=AzureServiceBusPasswordless(namespace="ns")
            ).is_valid_connection()
        )
        self.assertFalse(
            AzureServiceBusConnectionSettings(
                passwordless=AzureServiceBusPasswordless(namespace=" ")
            ).is_valid_connection()
        )

    def test_get_service_bus_url_extracts_endpoint(self):
        settings = AzureServiceBusConnectionSettings(
            connection_string="Endpoint=sb://my.servicebus.windows.net;SharedAccessKeyName=key"
        )
        self.assertEqual("sb://my.servicebus.windows.net", settings.get_service_bus_url())

    def test_get_service_bus_url_extraction_starts_at_found_index(self):
        settings = AzureServiceBusConnectionSettings(
            connection_string="SharedAccessKeyName=key;Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKey=abc"
        )
        self.assertEqual("sb://ns.servicebus.windows.net/", settings.get_service_bus_url())

    def test_get_service_bus_url_case_insensitive_extraction(self):
        settings = AzureServiceBusConnectionSettings(connection_string="Endpoint=SB://ns.servicebus.windows.net/")
        self.assertEqual("SB://ns.servicebus.windows.net/", settings.get_service_bus_url())

    def test_get_service_bus_url_to_end_without_semicolon(self):
        settings = AzureServiceBusConnectionSettings(connection_string="Endpoint=sb://ns.servicebus.windows.net")
        self.assertEqual("sb://ns.servicebus.windows.net", settings.get_service_bus_url())

    def test_get_service_bus_url_from_namespace(self):
        settings = AzureServiceBusConnectionSettings(
            entra_id=AzureServiceBusEntraId(namespace="ns", tenant_id="t", client_id="c", client_secret="s")
        )
        self.assertEqual("sb://ns/", settings.get_service_bus_url())
        settings = AzureServiceBusConnectionSettings(passwordless=AzureServiceBusPasswordless(namespace="ns"))
        self.assertEqual("sb://ns/", settings.get_service_bus_url())

    def test_get_service_bus_url_throws_when_no_endpoint(self):
        with self.assertRaises(InvalidOperationException) as ctx:
            AzureServiceBusConnectionSettings(connection_string="no sb here").get_service_bus_url()
        self.assertEqual("No endpoint provided", str(ctx.exception))

    def test_get_service_bus_url_throws_when_no_namespace(self):
        with self.assertRaises(InvalidOperationException) as ctx:
            AzureServiceBusConnectionSettings().get_service_bus_url()
        self.assertEqual("No namespace provided", str(ctx.exception))

    def test_to_json_writes_only_set_fields(self):
        self.assertEqual({}, AzureServiceBusConnectionSettings().to_json())
        settings = AzureServiceBusConnectionSettings(
            connection_string="Endpoint=sb://x/",
            entra_id=AzureServiceBusEntraId(namespace="ns", tenant_id="t", client_id="c", client_secret="s"),
        )
        result = settings.to_json()
        self.assertEqual({"Endpoint=sb://x/"}, {result["ConnectionString"]})
        self.assertIn("EntraId", result)
        self.assertNotIn("Passwordless", result)

    def test_audit_json_masks_secrets(self):
        settings = AzureServiceBusConnectionSettings(connection_string="Endpoint=sb://x/;Key=secret")
        self.assertEqual({"ConnectionString": "<Contains-Secrets>"}, settings.to_audit_json())

        settings = AzureServiceBusConnectionSettings(
            entra_id=AzureServiceBusEntraId(namespace="ns", tenant_id="t", client_id="c", client_secret="secret")
        )
        audit = settings.to_audit_json()
        self.assertNotIn("ClientSecret", audit["EntraId"])

        settings = AzureServiceBusConnectionSettings(passwordless=AzureServiceBusPasswordless(namespace="ns"))
        self.assertEqual({"Passwordless": {"Namespace": "ns"}}, settings.to_audit_json())

    def test_equality_and_hash(self):
        a = AzureServiceBusConnectionSettings(connection_string="Endpoint=sb://x/")
        b = AzureServiceBusConnectionSettings(connection_string="Endpoint=sb://x/")
        c = AzureServiceBusConnectionSettings(connection_string="Endpoint=sb://y/")
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)
        self.assertEqual(hash(a), hash(b))

        e1 = AzureServiceBusEntraId(namespace="ns", tenant_id="t", client_id="c", client_secret="s")
        e2 = AzureServiceBusEntraId(namespace="ns", tenant_id="t", client_id="c", client_secret="s")
        self.assertEqual(e1, e2)
        self.assertEqual(hash(e1), hash(e2))

        p1 = AzureServiceBusPasswordless(namespace="ns")
        p2 = AzureServiceBusPasswordless(namespace="ns")
        self.assertEqual(p1, p2)
        self.assertEqual(hash(p1), hash(p2))


class TestQueueConnectionStringAsbField(unittest.TestCase):
    def test_to_json_writes_key_between_amazon_sqs_and_type(self):
        connection_string = QueueConnectionString(name="q", broker_type=QueueBrokerType.AZURE_SERVICE_BUS)
        keys = list(connection_string.to_json().keys())
        self.assertEqual("AmazonSqsConnectionSettings", keys[-3])
        self.assertEqual("AzureServiceBusConnectionSettings", keys[-2])
        self.assertEqual("Type", keys[-1])

    def test_unset_field_is_null_on_wire(self):
        connection_string = QueueConnectionString(name="q", broker_type=QueueBrokerType.AZURE_SERVICE_BUS)
        self.assertIsNone(connection_string.to_json()["AzureServiceBusConnectionSettings"])

    def test_from_json_parses_full_settings_object(self):
        payload = {
            "Name": "q",
            "BrokerType": "AzureServiceBus",
            "KafkaConnectionSettings": None,
            "RabbitMqConnectionSettings": None,
            "AzureQueueStorageConnectionSettings": None,
            "AmazonSqsConnectionSettings": None,
            "AzureServiceBusConnectionSettings": {"ConnectionString": "Endpoint=sb://ns/"},  # type: ignore
            "Type": "Queue",
        }
        parsed = QueueConnectionString.from_json(payload)
        self.assertIsInstance(parsed.azure_service_bus_settings, AzureServiceBusConnectionSettings)
        self.assertEqual("Endpoint=sb://ns/", parsed.azure_service_bus_settings.connection_string)

    def test_round_trip(self):
        settings = AzureServiceBusConnectionSettings(connection_string="Endpoint=sb://ns/")
        original = QueueConnectionString(
            name="q", broker_type=QueueBrokerType.AZURE_SERVICE_BUS, azure_service_bus_settings=settings
        )
        back = QueueConnectionString.from_json(original.to_json())
        self.assertEqual(original.to_json(), back.to_json())


class TestAzureServiceBusSinkSource(unittest.TestCase):
    def test_queue_returns_name_when_valid(self):
        self.assertEqual("my-queue", AzureServiceBusSinkSource.queue("my-queue"))

    def test_queue_throws_when_empty(self):
        for value in (None, "", "   "):
            with self.assertRaises(ValueError) as ctx:
                AzureServiceBusSinkSource.queue(value)
            self.assertEqual("Queue name must be non-empty.", str(ctx.exception))

    def test_queue_throws_when_contains_separator(self):
        with self.assertRaises(ValueError) as ctx:
            AzureServiceBusSinkSource.queue("foo;bar")
        self.assertEqual("Queue name must not contain the ';' character.", str(ctx.exception))

    def test_subscription_encodes_topic_and_subscription(self):
        self.assertEqual("topic;sub", AzureServiceBusSinkSource.subscription("topic", "sub"))

    def test_subscription_throws_when_empty(self):
        for topic, subscription in (
            (None, "sub"),
            ("", "sub"),
            ("   ", "sub"),
            ("topic", None),
            ("topic", ""),
            ("topic", "  "),
        ):
            with self.assertRaises(ValueError):
                AzureServiceBusSinkSource.subscription(topic, subscription)

    def test_subscription_throws_when_contains_separator(self):
        with self.assertRaises(ValueError) as ctx:
            AzureServiceBusSinkSource.subscription("to;pic", "sub")
        self.assertEqual("Topic name must not contain the ';' character.", str(ctx.exception))
        with self.assertRaises(ValueError) as ctx:
            AzureServiceBusSinkSource.subscription("topic", "su;b")
        self.assertEqual("Subscription name must not contain the ';' character.", str(ctx.exception))

    def test_validate_entry_accepts_valid_entries(self):
        self.assertIsNone(AzureServiceBusSinkSource.validate_entry("my-queue"))
        self.assertIsNone(AzureServiceBusSinkSource.validate_entry("topic;sub"))

    def test_validate_entry_rejects_invalid_entries(self):
        for entry in ("", "   ", ";sub", "topic;", ";", "topic;sub;extra"):
            self.assertIsNotNone(AzureServiceBusSinkSource.validate_entry(entry), entry)

    def test_validate_script_names_script_and_entry(self):
        errors = AzureServiceBusSinkSource.validate_script("script", ["ok", "a;b;c"])
        self.assertEqual(
            [
                "Script 'script': Azure Service Bus subscription source 'a;b;c' is invalid. "
                "Use '<topic>;<subscription>' with a single ';' separator and both parts non-empty."
            ],
            errors,
        )
        errors = AzureServiceBusSinkSource.validate_script("script", ["  "])
        self.assertEqual(["Script 'script': Azure Service Bus source entry cannot be empty."], errors)
        self.assertEqual([], AzureServiceBusSinkSource.validate_script("script", ["my-queue"]))


if __name__ == "__main__":
    unittest.main()
