"""
Tests for the Azure Service Bus queue broker added in 7.2.5.
"""

import unittest

from ravendb.documents.operations.connection_string.get_connection_string_operation import (
    GetConnectionStringsOperation,
)
from ravendb.documents.operations.connection_string.put_connection_string_operation import (
    PutConnectionStringOperation,
)
from ravendb.documents.operations.connection_string.remove_connection_string_operation import (
    RemoveConnectionStringOperation,
)
from ravendb.documents.operations.etl.queue.azure_service_bus_connection_settings import (
    AzureServiceBusConnectionSettings,
    AzureServiceBusEntraId,
    AzureServiceBusPasswordless,
)
from ravendb.documents.operations.etl.queue.connection import QueueBrokerType, QueueConnectionString
from ravendb.serverwide.server_operation_executor import ConnectionStringType
from ravendb.tests.test_base import TestBase

CONNECTION_STRING = "Endpoint=sb://rvn-test.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=v"


class TestAzureServiceBusConnectionSettings(unittest.TestCase):
    def test_azure_service_bus_is_a_known_broker(self):
        self.assertEqual("AzureServiceBus", QueueBrokerType.AZURE_SERVICE_BUS.value)

    def test_connection_string_authentication_round_trips(self):
        settings = AzureServiceBusConnectionSettings(connection_string=CONNECTION_STRING)
        serialized = settings.to_json()

        self.assertEqual(CONNECTION_STRING, serialized["ConnectionString"])
        self.assertIsNone(serialized["EntraId"])
        self.assertIsNone(serialized["Passwordless"])
        self.assertEqual(serialized, AzureServiceBusConnectionSettings.from_json(serialized).to_json())

    def test_entra_id_authentication_round_trips(self):
        settings = AzureServiceBusConnectionSettings(
            entra_id=AzureServiceBusEntraId(
                namespace="rvn-test.servicebus.windows.net",
                tenant_id="tenant",
                client_id="client",
                client_secret="secret",
            )
        )
        parsed = AzureServiceBusConnectionSettings.from_json(settings.to_json())

        self.assertEqual("rvn-test.servicebus.windows.net", parsed.entra_id.namespace)
        self.assertEqual("tenant", parsed.entra_id.tenant_id)
        self.assertEqual("client", parsed.entra_id.client_id)
        self.assertEqual("secret", parsed.entra_id.client_secret)

    def test_passwordless_authentication_round_trips(self):
        settings = AzureServiceBusConnectionSettings(
            passwordless=AzureServiceBusPasswordless(namespace="rvn-test.servicebus.windows.net")
        )
        parsed = AzureServiceBusConnectionSettings.from_json(settings.to_json())

        self.assertEqual("rvn-test.servicebus.windows.net", parsed.passwordless.namespace)
        self.assertIsNone(parsed.entra_id)

    def test_the_settings_hang_off_the_queue_connection_string(self):
        connection_string = QueueConnectionString(
            name="asb1",
            broker_type=QueueBrokerType.AZURE_SERVICE_BUS,
            azure_service_bus_settings=AzureServiceBusConnectionSettings(connection_string=CONNECTION_STRING),
        )
        serialized = connection_string.to_json()

        self.assertEqual("AzureServiceBus", serialized["BrokerType"])
        self.assertEqual(CONNECTION_STRING, serialized["AzureServiceBusConnectionSettings"]["ConnectionString"])

    def test_a_queue_connection_string_from_an_older_server_has_no_service_bus_settings(self):
        parsed = QueueConnectionString.from_json(
            {
                "Name": "kafka1",
                "BrokerType": "Kafka",
                "KafkaConnectionSettings": {"BootstrapServers": "localhost:9092"},
                "RabbitMqConnectionSettings": None,
                "AzureQueueStorageConnectionSettings": None,
                "AmazonSqsConnectionSettings": None,
            }
        )

        self.assertIsNone(parsed.azure_service_bus_settings)


class TestAzureServiceBusConnectionStringLifecycle(TestBase):
    def setUp(self):
        super().setUp()

    def test_the_server_stores_and_returns_an_azure_service_bus_connection_string(self):
        connection_string = QueueConnectionString(
            name="asb1",
            broker_type=QueueBrokerType.AZURE_SERVICE_BUS,
            azure_service_bus_settings=AzureServiceBusConnectionSettings(connection_string=CONNECTION_STRING),
        )

        put_result = self.store.maintenance.send(PutConnectionStringOperation(connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        get_result = self.store.maintenance.send(GetConnectionStringsOperation("asb1", ConnectionStringType.QUEUE))
        stored = get_result.queue_connection_strings["asb1"]
        self.assertEqual(QueueBrokerType.AZURE_SERVICE_BUS, stored.broker_type)
        self.assertEqual(CONNECTION_STRING, stored.azure_service_bus_settings.connection_string)

        remove_result = self.store.maintenance.send(RemoveConnectionStringOperation(connection_string))
        self.assertGreater(remove_result.raft_command_index, 0)

        after_delete = self.store.maintenance.send(GetConnectionStringsOperation("asb1", ConnectionStringType.QUEUE))
        self.assertFalse(after_delete.queue_connection_strings)

    def test_entra_id_credentials_survive_a_server_round_trip(self):
        connection_string = QueueConnectionString(
            name="asb2",
            broker_type=QueueBrokerType.AZURE_SERVICE_BUS,
            azure_service_bus_settings=AzureServiceBusConnectionSettings(
                entra_id=AzureServiceBusEntraId(
                    namespace="rvn-test.servicebus.windows.net",
                    tenant_id="tenant",
                    client_id="client",
                    client_secret="secret",
                )
            ),
        )

        self.store.maintenance.send(PutConnectionStringOperation(connection_string))
        stored = self.store.maintenance.send(
            GetConnectionStringsOperation("asb2", ConnectionStringType.QUEUE)
        ).queue_connection_strings["asb2"]

        self.assertEqual("rvn-test.servicebus.windows.net", stored.azure_service_bus_settings.entra_id.namespace)
        self.assertEqual("client", stored.azure_service_bus_settings.entra_id.client_id)
