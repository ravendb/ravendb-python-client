"""Tests for the server-wide connection strings surface:
the wrapper class, and the put/get/remove operations.
"""

import json
import unittest

from ravendb.documents.operations.etl.queue.connection import QueueBrokerType, QueueConnectionString
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.serverwide.operations.connection_strings import (
    GetServerWideConnectionStringsOperation,
    PutServerWideConnectionStringOperation,
    PutServerWideConnectionStringResult,
    RemoveServerWideConnectionStringOperation,
    RemoveServerWideConnectionStringResult,
    ServerWideConnectionString,
    ServerWideConnectionStringUsage,
)
from ravendb.serverwide.server_operation_executor import ConnectionStringType


class TestServerWideConnectionString(unittest.TestCase):
    def _queue(self):
        return QueueConnectionString(name="q1", broker_type=QueueBrokerType.KAFKA)

    def test_to_json_inner_keys_plus_type_and_excluded(self):
        wrapper = ServerWideConnectionString(connection_string=self._queue(), excluded_databases=["db2"])
        result = wrapper.to_json()
        self.assertEqual("Queue", result["Type"])
        self.assertEqual(["db2"], result["ExcludedDatabases"])
        self.assertEqual("Kafka", result["BrokerType"])
        self.assertNotIn("UsedBy", result)
        self.assertEqual(1, len([k for k in result if k == "Type"]))

    def test_excluded_databases_null_when_unset(self):
        wrapper = ServerWideConnectionString(connection_string=self._queue())
        self.assertIsNone(wrapper.to_json()["ExcludedDatabases"])

    def test_name_and_type_delegate_to_inner(self):
        wrapper = ServerWideConnectionString(connection_string=self._queue())
        self.assertEqual("q1", wrapper.name)
        self.assertEqual("Queue", wrapper.get_type)

    def test_none_inner_delegates_to_none(self):
        wrapper = ServerWideConnectionString()
        self.assertIsNone(wrapper.name)
        self.assertEqual("None", wrapper.get_type)
        self.assertEqual({"Type": "None", "ExcludedDatabases": None}, wrapper.to_json())

    def test_from_json_dispatches_by_type(self):
        item = {
            "Name": "q1",
            "BrokerType": "Kafka",
            "KafkaConnectionSettings": None,
            "RabbitMqConnectionSettings": None,
            "AzureQueueStorageConnectionSettings": None,
            "AmazonSqsConnectionSettings": None,
            "AzureServiceBusConnectionSettings": None,
            "Type": "Queue",
            "ExcludedDatabases": ["db2"],
            "UsedBy": [
                {
                    "Kind": "QueueSink",
                    "Id": 99999999999,
                    "Identifier": "ident",
                    "Name": "task",
                    "DatabaseName": "db1",
                }
            ],
        }
        parsed = ServerWideConnectionString.from_json(item)
        self.assertIsInstance(parsed, ServerWideConnectionString)
        self.assertEqual("q1", parsed.name)
        self.assertEqual("Queue", parsed.get_type)
        self.assertEqual(["db2"], parsed.excluded_databases)
        self.assertIsInstance(parsed.connection_string, QueueConnectionString)
        usage = parsed.used_by[0]
        self.assertIsInstance(usage, ServerWideConnectionStringUsage)
        self.assertEqual("QueueSink", usage.kind)
        self.assertEqual(99999999999, usage.id)
        self.assertEqual("db1", usage.database_name)

    def test_from_json_returns_none_when_type_missing(self):
        self.assertIsNone(ServerWideConnectionString.from_json({"Name": "x"}))

    def test_from_json_tolerates_missing_used_by(self):
        parsed = ServerWideConnectionString.from_json(
            {
                "Name": "r1",
                "Database": "d",
                "TopologyDiscoveryUrls": ["http://localhost:8080"],
                "Type": "Raven",
            }
        )
        self.assertEqual([], parsed.used_by)


class TestPutServerWideConnectionStringOperation(unittest.TestCase):
    def setUp(self):
        self.node = ServerNode("http://localhost:8080", "db1")

    def _wrapper(self):
        inner = QueueConnectionString(name="q1", broker_type=QueueBrokerType.KAFKA)
        return ServerWideConnectionString(connection_string=inner, excluded_databases=["db2"])

    def test_put_url_and_body(self):
        operation = PutServerWideConnectionStringOperation(self._wrapper())
        command = operation.get_command(None)
        request = command.create_request(self.node)
        self.assertEqual("PUT", request.method)
        self.assertEqual("http://localhost:8080/admin/configuration/server-wide/connection-strings", request.url)
        body = json.loads(request.data)
        self.assertEqual("Queue", body["Type"])
        self.assertEqual(["db2"], body["ExcludedDatabases"])
        self.assertNotIn("UsedBy", body)

    def test_not_a_read_request(self):
        command = PutServerWideConnectionStringOperation(self._wrapper()).get_command(None)
        self.assertFalse(command.is_read_request())

    def test_implements_raft_command(self):
        command = PutServerWideConnectionStringOperation(self._wrapper()).get_command(None)
        self.assertIsInstance(command, RaftCommand)
        self.assertTrue(command.get_raft_unique_request_id())

    def test_null_response_raises(self):
        command = PutServerWideConnectionStringOperation(self._wrapper()).get_command(None)
        with self.assertRaises(ValueError):
            command.set_response(None, False)

    def test_result_parses_raft_command_index(self):
        result = PutServerWideConnectionStringResult.from_json({"RaftCommandIndex": 1234567890123})
        self.assertEqual(1234567890123, result.raft_command_index)

    def test_constructor_validation(self):
        with self.assertRaises(ValueError):
            PutServerWideConnectionStringOperation(None)
        with self.assertRaises(ValueError):
            PutServerWideConnectionStringOperation(ServerWideConnectionString())


class TestGetServerWideConnectionStringsOperation(unittest.TestCase):
    def setUp(self):
        self.node = ServerNode("http://localhost:8080", "db1")

    def test_no_query_when_no_params(self):
        operation = GetServerWideConnectionStringsOperation()
        request = operation.get_command(None).create_request(self.node)
        self.assertEqual("GET", request.method)
        self.assertEqual("http://localhost:8080/admin/configuration/server-wide/connection-strings", request.url)

    def test_name_then_type_query_order(self):
        operation = GetServerWideConnectionStringsOperation("my cs", ConnectionStringType.QUEUE)
        request = operation.get_command(None).create_request(self.node)
        self.assertEqual(
            "http://localhost:8080/admin/configuration/server-wide/connection-strings?name=my%20cs&type=Queue",
            request.url,
        )

    def test_type_only_query(self):
        operation = GetServerWideConnectionStringsOperation(None, ConnectionStringType.QUEUE)
        request = operation.get_command(None).create_request(self.node)
        self.assertEqual(
            "http://localhost:8080/admin/configuration/server-wide/connection-strings?type=Queue", request.url
        )

    def test_type_value_is_enum_name_not_repr(self):
        operation = GetServerWideConnectionStringsOperation("n", ConnectionStringType.QUEUE)
        url = operation.get_command(None).create_request(self.node).url
        self.assertNotIn("ConnectionStringType.QUEUE", url)
        self.assertIn("type=Queue", url)

    def test_blank_name_raises(self):
        with self.assertRaises(ValueError) as ctx:
            GetServerWideConnectionStringsOperation("   ")
        self.assertIn("Connection string name must not be null or empty", str(ctx.exception))

    def test_is_read_request(self):
        command = GetServerWideConnectionStringsOperation().get_command(None)
        self.assertTrue(command.is_read_request())

    def test_null_response_raises(self):
        command = GetServerWideConnectionStringsOperation().get_command(None)
        with self.assertRaises(ValueError):
            command.set_response(None, False)

    def test_result_parses_results_with_database_name(self):
        payload = {
            "Results": [
                {
                    "Name": "q1",
                    "BrokerType": "Kafka",
                    "KafkaConnectionSettings": None,
                    "RabbitMqConnectionSettings": None,
                    "AzureQueueStorageConnectionSettings": None,
                    "AmazonSqsConnectionSettings": None,
                    "AzureServiceBusConnectionSettings": None,
                    "Type": "Queue",
                    "ExcludedDatabases": None,
                    "UsedBy": [{"Kind": "QueueSink", "Id": 1, "Identifier": None, "Name": "t", "DatabaseName": "db"}],
                }
            ]
        }
        command = GetServerWideConnectionStringsOperation().get_command(None)
        command.set_response(json.dumps(payload), False)
        self.assertEqual(1, len(command.result.results))
        self.assertEqual("q1", command.result.results[0].name)
        self.assertEqual("db", command.result.results[0].used_by[0].database_name)


class TestRemoveServerWideConnectionStringOperation(unittest.TestCase):
    def setUp(self):
        self.node = ServerNode("http://localhost:8080", "db1")

    def test_delete_url_with_name_and_type(self):
        inner = QueueConnectionString(name="q1", broker_type=QueueBrokerType.KAFKA)
        operation = RemoveServerWideConnectionStringOperation(inner)
        command = operation.get_command(None)
        request = command.create_request(self.node)
        self.assertEqual("DELETE", request.method)
        self.assertEqual(
            "http://localhost:8080/admin/configuration/server-wide/connection-strings?name=q1&type=Queue",
            request.url,
        )

    def test_implements_raft_command(self):
        inner = QueueConnectionString(name="q1", broker_type=QueueBrokerType.KAFKA)
        command = RemoveServerWideConnectionStringOperation(inner).get_command(None)
        self.assertIsInstance(command, RaftCommand)

    def test_constructor_validation(self):
        with self.assertRaises(ValueError):
            RemoveServerWideConnectionStringOperation(None)
        with self.assertRaises(ValueError):
            RemoveServerWideConnectionStringOperation(QueueConnectionString(name="  "))

    def test_result_parses_raft_command_index(self):
        result = RemoveServerWideConnectionStringResult.from_json({"RaftCommandIndex": 7})
        self.assertEqual(7, result.raft_command_index)


if __name__ == "__main__":
    unittest.main()
