"""Azure Service Bus queue connection string tests."""

import os
import unittest

from ravendb.documents.operations.connection_string.put_connection_string_operation import (
    PutConnectionStringOperation,
)
from ravendb.documents.operations.connection_string.get_connection_string_operation import (
    GetConnectionStringsOperation,
)
from ravendb.documents.operations.etl.queue.azure_service_bus_connection_settings import (
    AzureServiceBusConnectionSettings,
    AzureServiceBusEntraId,
    AzureServiceBusPasswordless,
)
from ravendb.documents.operations.etl.queue.connection import (
    QueueBrokerType,
    QueueConnectionString,
)
from ravendb.serverwide.server_operation_executor import ConnectionStringType
from ravendb.tests.test_base import TestBase


class TestAzureServiceBusConnectionSettings(unittest.TestCase):
    def test_to_json_omits_empty_connection_string(self):
        settings = AzureServiceBusConnectionSettings(connection_string="")
        self.assertEqual({}, settings.to_json())

    def test_to_json_with_connection_string(self):
        settings = AzureServiceBusConnectionSettings(
            connection_string="Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=s="
        )
        self.assertEqual(
            settings.to_json(),
            {"ConnectionString": "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=s="},
        )

    def test_to_json_with_entra_id_and_passwordless(self):
        settings = AzureServiceBusConnectionSettings(
            entra_id=AzureServiceBusEntraId(
                namespace="ns.servicebus.windows.net",
                tenant_id="t",
                client_id="c",
                client_secret="s",
            ),
            passwordless=AzureServiceBusPasswordless(namespace="ns.servicebus.windows.net"),
        )
        payload = settings.to_json()
        self.assertEqual(
            payload["EntraId"],
            {
                "Namespace": "ns.servicebus.windows.net",
                "TenantId": "t",
                "ClientId": "c",
                "ClientSecret": "s",
            },
        )
        self.assertEqual(payload["Passwordless"], {"Namespace": "ns.servicebus.windows.net"})

    def test_queue_connection_string_wire(self):
        connection_string = QueueConnectionString(
            name="asb-cs",
            broker_type=QueueBrokerType.AZURE_SERVICE_BUS,
            azure_service_bus_settings=AzureServiceBusConnectionSettings(
                passwordless=AzureServiceBusPasswordless(namespace="ns.servicebus.windows.net")
            ),
        )
        payload = connection_string.to_json()
        self.assertEqual("AzureServiceBus", payload["BrokerType"])
        self.assertEqual(
            {"Passwordless": {"Namespace": "ns.servicebus.windows.net"}},
            payload["AzureServiceBusConnectionSettings"],
        )
        parsed = QueueConnectionString.from_json(payload)
        self.assertEqual(QueueBrokerType.AZURE_SERVICE_BUS, parsed.broker_type)
        self.assertEqual(
            "ns.servicebus.windows.net",
            parsed.azure_service_bus_settings.passwordless.namespace,
        )


@unittest.skipIf(
    os.environ.get("RAVENDB_LICENSE") is None and os.environ.get("RAVEN_License") is None,
    "Insufficient license permissions. Skipping on CI/CD.",
)
class TestAzureServiceBusRoundTrip(TestBase):
    def test_put_and_get_queue_connection_string(self):
        connection_string = QueueConnectionString(
            name="asb-cs",
            broker_type=QueueBrokerType.AZURE_SERVICE_BUS,
            azure_service_bus_settings=AzureServiceBusConnectionSettings(
                passwordless=AzureServiceBusPasswordless(namespace="ns.servicebus.windows.net")
            ),
        )
        self.store.maintenance.send(PutConnectionStringOperation(connection_string))

        result = self.store.maintenance.send(
            GetConnectionStringsOperation(
                connection_string_name="asb-cs",
                connection_string_type=ConnectionStringType.QUEUE,
            )
        )
        parsed = result.queue_connection_strings["asb-cs"]
        self.assertEqual(QueueBrokerType.AZURE_SERVICE_BUS, parsed.broker_type)
        self.assertEqual(
            "ns.servicebus.windows.net",
            parsed.azure_service_bus_settings.passwordless.namespace,
        )
