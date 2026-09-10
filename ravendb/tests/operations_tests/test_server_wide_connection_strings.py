"""
Tests for the server-wide connection string operations added in 7.2.5, and for the
UsedBy metadata the server now returns with every connection string.
"""

import json
import os
import unittest

from ravendb.documents.operations.connection_string.get_connection_string_operation import (
    GetConnectionStringsResult,
)
from ravendb.documents.operations.connection_strings import ConnectionStringUsage, ConnectionStringUsageKind
from ravendb.documents.operations.etl.configuration import RavenConnectionString
from ravendb.documents.operations.etl.queue.connection import QueueBrokerType, QueueConnectionString
from ravendb.documents.operations.etl.sql import SqlConnectionString
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.serverwide.operations.connection_strings import (
    GetServerWideConnectionStringsOperation,
    PutServerWideConnectionStringOperation,
    RemoveServerWideConnectionStringOperation,
    ServerWideConnectionString,
    ServerWideConnectionStringUsage,
)
from ravendb.serverwide.server_operation_executor import ConnectionStringType
from ravendb.tests.test_base import TestBase


class TestConnectionStringUsage(unittest.TestCase):
    def test_ongoing_tasks_are_identified_by_a_numeric_id(self):
        usage = ConnectionStringUsage.from_json({"Kind": "RavenEtl", "Id": 3, "Identifier": None, "Name": "etl-to-b"})

        self.assertEqual(ConnectionStringUsageKind.RAVEN_ETL, usage.kind)
        self.assertEqual(3, usage.id_)
        self.assertIsNone(usage.identifier)
        self.assertEqual("etl-to-b", usage.name)

    def test_ai_agents_are_identified_by_a_string_identifier(self):
        usage = ConnectionStringUsage.from_json(
            {"Kind": "AiAgent", "Id": None, "Identifier": "agents/support", "Name": "support"}
        )

        self.assertEqual(ConnectionStringUsageKind.AI_AGENT, usage.kind)
        self.assertIsNone(usage.id_)
        self.assertEqual("agents/support", usage.identifier)

    def test_cdc_sink_is_a_known_usage_kind(self):
        self.assertEqual("CdcSink", ConnectionStringUsageKind.CDC_SINK.value)

    def test_read_connection_strings_carry_their_usages(self):
        result = GetConnectionStringsResult.from_json(
            {
                "RavenConnectionStrings": {
                    "to-b": {
                        "Name": "to-b",
                        "Database": "b",
                        "TopologyDiscoveryUrls": ["http://localhost:8080"],
                        "UsedBy": [
                            {"Kind": "RavenEtl", "Id": 3, "Name": "etl-1"},
                            {"Kind": "AiAgent", "Identifier": "agents/1", "Name": "helper"},
                        ],
                    }
                }
            }
        )
        connection_string = result.raven_connection_strings["to-b"]

        self.assertEqual(2, len(connection_string.used_by))
        self.assertEqual(ConnectionStringUsageKind.RAVEN_ETL, connection_string.used_by[0].kind)
        self.assertEqual("agents/1", connection_string.used_by[1].identifier)

    def test_a_connection_string_nothing_uses_has_an_empty_usage_list(self):
        result = GetConnectionStringsResult.from_json(
            {"SqlConnectionStrings": {"sql1": {"Name": "sql1", "ConnectionString": "x", "FactoryName": "y"}}}
        )

        self.assertEqual([], result.sql_connection_strings["sql1"].used_by)

    def test_usages_are_never_written_back(self):
        # UsedBy is computed server-side; sending it back would be meaningless.
        connection_string = RavenConnectionString("to-b", "b", ["http://localhost:8080"])
        connection_string.used_by = [ConnectionStringUsage(ConnectionStringUsageKind.RAVEN_ETL, 3, None, "etl-1")]

        self.assertNotIn("UsedBy", connection_string.to_json())

    def test_an_absent_bucket_reads_as_none(self):
        result = GetConnectionStringsResult.from_json({"RavenConnectionStrings": {}})

        self.assertIsNone(result.raven_connection_strings)
        self.assertIsNone(result.queue_connection_strings)


class TestServerWideConnectionString(unittest.TestCase):
    def _raven(self) -> ServerWideConnectionString:
        return ServerWideConnectionString(
            connection_string=RavenConnectionString("shared-raven", "orders", ["http://localhost:8080"]),
            excluded_databases=["scratch"],
        )

    def test_name_and_type_are_delegated_to_the_wrapped_connection_string(self):
        server_wide = self._raven()

        self.assertEqual("shared-raven", server_wide.name)
        self.assertEqual(ConnectionStringType.RAVEN, server_wide.type)

    def test_an_empty_wrapper_reports_no_type(self):
        server_wide = ServerWideConnectionString()

        self.assertIsNone(server_wide.name)
        self.assertEqual(ConnectionStringType.NONE, server_wide.type)

    def test_the_connection_string_is_flattened_rather_than_nested(self):
        serialized = self._raven().to_json()

        self.assertEqual("shared-raven", serialized["Name"])
        self.assertEqual("orders", serialized["Database"])
        self.assertEqual("Raven", serialized["Type"])
        self.assertEqual(["scratch"], serialized["ExcludedDatabases"])

    def test_it_round_trips_through_the_flat_shape(self):
        serialized = self._raven().to_json()
        parsed = ServerWideConnectionString.from_json(serialized)

        self.assertIsInstance(parsed.connection_string, RavenConnectionString)
        self.assertEqual(serialized, parsed.to_json())

    def test_every_connection_string_type_is_dispatched_by_its_type_field(self):
        queue = ServerWideConnectionString(
            connection_string=QueueConnectionString("shared-queue", QueueBrokerType.AZURE_SERVICE_BUS)
        )
        parsed = ServerWideConnectionString.from_json(queue.to_json())

        self.assertIsInstance(parsed.connection_string, QueueConnectionString)
        self.assertEqual(QueueBrokerType.AZURE_SERVICE_BUS, parsed.connection_string.broker_type)

    def test_a_payload_without_a_type_cannot_be_parsed(self):
        self.assertIsNone(ServerWideConnectionString.from_json({"Name": "x"}))
        self.assertIsNone(ServerWideConnectionString.from_json(None))

    def test_usages_carry_the_database_they_come_from(self):
        parsed = ServerWideConnectionString.from_json(
            {
                "Name": "shared-raven",
                "Database": "orders",
                "TopologyDiscoveryUrls": [],
                "Type": "Raven",
                "UsedBy": [{"Kind": "RavenEtl", "Id": 3, "Name": "etl-1", "DatabaseName": "shop"}],
            }
        )

        self.assertIsInstance(parsed.used_by[0], ServerWideConnectionStringUsage)
        self.assertEqual("shop", parsed.used_by[0].database_name)
        self.assertEqual(ConnectionStringUsageKind.RAVEN_ETL, parsed.used_by[0].kind)

    def test_the_propagated_name_inside_a_database_record_is_prefixed(self):
        self.assertEqual(
            "Server Wide Connection String, shared-raven",
            ServerWideConnectionString.get_database_record_connection_string_name("shared-raven"),
        )


class TestServerWideConnectionStringOperations(unittest.TestCase):
    def setUp(self):
        self.node = ServerNode("http://localhost:8080", "db")
        self.server_wide = ServerWideConnectionString(
            connection_string=RavenConnectionString("shared-raven", "orders", ["http://localhost:8080"])
        )

    def test_get_without_a_filter_asks_for_everything(self):
        command = GetServerWideConnectionStringsOperation().get_command(None)
        request = command.create_request(self.node)

        self.assertEqual("GET", request.method)
        self.assertEqual("http://localhost:8080/admin/configuration/server-wide/connection-strings", request.url)
        self.assertTrue(command.is_read_request())

    def test_get_filters_by_name_and_type(self):
        command = GetServerWideConnectionStringsOperation("shared raven", ConnectionStringType.RAVEN).get_command(None)
        url = command.create_request(self.node).url

        self.assertIn("?name=shared%20raven", url)
        self.assertIn("&type=Raven", url)

    def test_a_blank_name_is_rejected_client_side(self):
        with self.assertRaises(ValueError):
            GetServerWideConnectionStringsOperation("   ")

    def test_get_parses_the_results_list(self):
        command = GetServerWideConnectionStringsOperation().get_command(None)
        command.set_response(
            json.dumps(
                {
                    "Results": [
                        {
                            "Name": "shared-raven",
                            "Database": "orders",
                            "TopologyDiscoveryUrls": [],
                            "Type": "Raven",
                            "ExcludedDatabases": ["scratch"],
                        }
                    ]
                }
            ),
            False,
        )

        self.assertEqual(1, len(command.result.results))
        self.assertEqual("shared-raven", command.result.results[0].name)
        self.assertEqual(["scratch"], command.result.results[0].excluded_databases)

    def test_put_sends_the_flattened_connection_string(self):
        command = PutServerWideConnectionStringOperation(self.server_wide).get_command(None)
        request = command.create_request(self.node)

        self.assertEqual("PUT", request.method)
        self.assertEqual("http://localhost:8080/admin/configuration/server-wide/connection-strings", request.url)
        self.assertEqual(self.server_wide.to_json(), request.data)
        self.assertIsInstance(command, RaftCommand)

    def test_put_reads_back_the_raft_index(self):
        command = PutServerWideConnectionStringOperation(self.server_wide).get_command(None)
        command.set_response(json.dumps({"RaftCommandIndex": 11}), False)

        self.assertEqual(11, command.result.raft_command_index)

    def test_put_requires_a_wrapped_connection_string(self):
        with self.assertRaises(ValueError):
            PutServerWideConnectionStringOperation(None)
        with self.assertRaises(ValueError):
            PutServerWideConnectionStringOperation(ServerWideConnectionString())

    def test_remove_addresses_the_connection_string_by_name_and_type(self):
        command = RemoveServerWideConnectionStringOperation(SqlConnectionString("shared sql")).get_command(None)
        request = command.create_request(self.node)

        self.assertEqual("DELETE", request.method)
        self.assertIn("?name=shared%20sql", request.url)
        self.assertIn("&type=Sql", request.url)
        self.assertIsInstance(command, RaftCommand)

    def test_remove_requires_a_named_connection_string(self):
        with self.assertRaises(ValueError):
            RemoveServerWideConnectionStringOperation(None)
        with self.assertRaises(ValueError):
            RemoveServerWideConnectionStringOperation(SqlConnectionString(None))

    def test_remove_reads_back_the_raft_index(self):
        command = RemoveServerWideConnectionStringOperation(SqlConnectionString("sql1")).get_command(None)
        command.set_response(json.dumps({"RaftCommandIndex": 12}), False)

        self.assertEqual(12, command.result.raft_command_index)


@unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
class TestServerWideConnectionStringsAgainstServer(TestBase):
    # Server-wide connection strings are licensed: writing one hits the license gate on an
    # unlicensed server, so these only run when a license is configured.

    def _connection_string(self, name: str) -> ServerWideConnectionString:
        return ServerWideConnectionString(
            RavenConnectionString(name, database="db1", topology_discovery_urls=[self.store.urls[0]])
        )

    def test_a_connection_string_is_stored_listed_and_removed(self):
        put = self.store.maintenance.server.send(
            PutServerWideConnectionStringOperation(self._connection_string("sw-raven"))
        )
        self.assertGreater(put.raft_command_index, 0)

        listed = self.store.maintenance.server.send(GetServerWideConnectionStringsOperation())
        self.assertIn("sw-raven", [result.name for result in listed.results])

        removed = self.store.maintenance.server.send(
            RemoveServerWideConnectionStringOperation(RavenConnectionString("sw-raven"))
        )
        self.assertGreater(removed.raft_command_index, 0)

        listed = self.store.maintenance.server.send(GetServerWideConnectionStringsOperation())
        self.assertNotIn("sw-raven", [result.name for result in listed.results])

    def test_a_stored_connection_string_reads_back_with_its_type_and_urls(self):
        self.store.maintenance.server.send(PutServerWideConnectionStringOperation(self._connection_string("sw-typed")))
        try:
            listed = self.store.maintenance.server.send(GetServerWideConnectionStringsOperation())
            stored = next(result for result in listed.results if result.name == "sw-typed")

            self.assertEqual(ConnectionStringType.RAVEN, stored.type)
            self.assertIsInstance(stored.connection_string, RavenConnectionString)
            self.assertEqual("db1", stored.connection_string.database)
            self.assertEqual([self.store.urls[0]], stored.connection_string.topology_discovery_urls)
            # Nothing references it yet.
            self.assertEqual([], stored.used_by)
        finally:
            self.store.maintenance.server.send(
                RemoveServerWideConnectionStringOperation(RavenConnectionString("sw-typed"))
            )

    def test_filtering_by_name_and_type_narrows_the_listing(self):
        for name in ("sw-one", "sw-two"):
            self.store.maintenance.server.send(PutServerWideConnectionStringOperation(self._connection_string(name)))
        try:
            listed = self.store.maintenance.server.send(
                GetServerWideConnectionStringsOperation("sw-one", ConnectionStringType.RAVEN)
            )

            self.assertEqual(["sw-one"], [result.name for result in listed.results])
        finally:
            for name in ("sw-one", "sw-two"):
                self.store.maintenance.server.send(
                    RemoveServerWideConnectionStringOperation(RavenConnectionString(name))
                )
