"""
Tests for the Queue Sink client surface: the configuration, the Add/Update operations,
the Azure Service Bus source encoding, and the QueueSink ongoing task.
"""

import json
import os
import unittest

from ravendb.documents.operations.etl.queue.connection import QueueBrokerType
from ravendb.documents.operations.ongoing_tasks import (
    GetOngoingTaskInfoOperation,
    OngoingTaskQueueSink,
    OngoingTaskType,
)
from ravendb.documents.operations.queue_sink import (
    AddQueueSinkOperation,
    AddQueueSinkOperationResult,
    AzureServiceBusSinkSource,
    QueueSinkConfiguration,
    QueueSinkProcessState,
    QueueSinkScript,
    UpdateQueueSinkOperation,
)
from ravendb.documents.operations.connection_string.put_connection_string_operation import (
    PutConnectionStringOperation,
)
from ravendb.documents.operations.etl.queue.connection import QueueConnectionString
from ravendb.documents.operations.etl.queue.kafka_connection_settings import KafkaConnectionSettings
from ravendb.exceptions.raven_exceptions import RavenException
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.serverwide.database_record import DatabaseRecord
from ravendb.tests.test_base import TestBase


def _configuration() -> QueueSinkConfiguration:
    return QueueSinkConfiguration(
        name="orders-sink",
        broker_type=QueueBrokerType.KAFKA,
        connection_string_name="kafka",
        scripts=[
            QueueSinkScript(
                name="orders",
                queues=["orders-topic"],
                script="put('orders/', this);",
            )
        ],
    )


class TestQueueSinkConfiguration(unittest.TestCase):
    def test_configuration_survives_a_json_round_trip(self):
        serialized = _configuration().to_json()

        self.assertEqual(serialized, QueueSinkConfiguration.from_json(serialized).to_json())

    def test_configuration_serializes_the_task_level_fields(self):
        serialized = _configuration().to_json()

        self.assertEqual("orders-sink", serialized["Name"])
        self.assertEqual("Kafka", serialized["BrokerType"])
        self.assertEqual("kafka", serialized["ConnectionStringName"])
        self.assertEqual(0, serialized["TaskId"])
        self.assertFalse(serialized["Disabled"])

    def test_a_script_carries_its_queues_and_disabled_flag(self):
        # The wire format comes from reflection over the public properties, so Disabled
        # ships even though the server-side ToJson leaves it out.
        script = QueueSinkScript("s1", ["q1", "q2"], "put('x/', this);", disabled=True)
        serialized = script.to_json()

        self.assertEqual(["q1", "q2"], serialized["Queues"])
        self.assertTrue(serialized["Disabled"])
        self.assertEqual(serialized, QueueSinkScript.from_json(serialized).to_json())

    def test_collections_default_to_empty_rather_than_none(self):
        self.assertEqual([], QueueSinkConfiguration().scripts)
        self.assertEqual([], QueueSinkScript().queues)

    def test_a_configuration_from_an_older_server_parses(self):
        configuration = QueueSinkConfiguration.from_json({"Name": "sink", "BrokerType": "RabbitMq"})

        self.assertEqual(QueueBrokerType.RABBIT_MQ, configuration.broker_type)
        self.assertEqual([], configuration.scripts)

    def test_process_state_item_name_is_lower_cased(self):
        # The server stores the cluster value under a lower-cased key.
        self.assertEqual(
            "values/DB/queuesink/orders-sink/orders",
            QueueSinkProcessState.generate_item_name("DB", "Orders-Sink", "Orders"),
        )

    def test_process_state_round_trips(self):
        payload = {"ConfigurationName": "orders-sink", "ScriptName": "orders", "NodeTag": "A"}

        self.assertEqual(payload, QueueSinkProcessState.from_json(payload).to_json())


class TestAzureServiceBusSinkSource(unittest.TestCase):
    def test_a_queue_entry_is_the_queue_name(self):
        self.assertEqual("orders", AzureServiceBusSinkSource.queue("orders"))

    def test_a_subscription_entry_joins_topic_and_subscription(self):
        self.assertEqual("events;audit", AzureServiceBusSinkSource.subscription("events", "audit"))

    def test_an_empty_name_is_rejected(self):
        with self.assertRaises(ValueError):
            AzureServiceBusSinkSource.queue("")
        with self.assertRaises(ValueError):
            AzureServiceBusSinkSource.queue("   ")
        with self.assertRaises(ValueError):
            AzureServiceBusSinkSource.subscription("events", "")
        with self.assertRaises(ValueError):
            AzureServiceBusSinkSource.subscription("", "audit")

    def test_the_separator_cannot_appear_inside_a_name(self):
        # Service Bus forbids ';' in names, so an entry carrying one could not be decoded.
        with self.assertRaises(ValueError):
            AzureServiceBusSinkSource.queue("a;b")
        with self.assertRaises(ValueError):
            AzureServiceBusSinkSource.subscription("a;b", "audit")
        with self.assertRaises(ValueError):
            AzureServiceBusSinkSource.subscription("events", "a;b")


class TestQueueSinkOperations(unittest.TestCase):
    def setUp(self):
        self.node = ServerNode("http://localhost:8080", "db")

    def test_add_sends_the_configuration_to_the_admin_endpoint(self):
        configuration = _configuration()
        command = AddQueueSinkOperation(configuration).get_command(None)
        request = command.create_request(self.node)

        self.assertEqual("PUT", request.method)
        self.assertEqual("http://localhost:8080/databases/db/admin/queue-sink", request.url)
        self.assertEqual(configuration.to_json(), request.data)
        self.assertFalse(command.is_read_request())

    def test_update_addresses_the_task_by_id(self):
        command = UpdateQueueSinkOperation(11, _configuration()).get_command(None)

        self.assertEqual(
            "http://localhost:8080/databases/db/admin/queue-sink?id=11",
            command.create_request(self.node).url,
        )

    def test_both_operations_are_raft_commands(self):
        add = AddQueueSinkOperation(_configuration()).get_command(None)
        update = UpdateQueueSinkOperation(1, _configuration()).get_command(None)

        self.assertIsInstance(add, RaftCommand)
        self.assertIsInstance(update, RaftCommand)
        self.assertTrue(add.get_raft_unique_request_id())

    def test_a_missing_configuration_is_rejected_client_side(self):
        with self.assertRaises(ValueError):
            AddQueueSinkOperation(None)
        with self.assertRaises(ValueError):
            UpdateQueueSinkOperation(1, None)

    def test_add_reads_the_task_id_off_the_response(self):
        command = AddQueueSinkOperation(_configuration()).get_command(None)
        command.set_response(json.dumps({"RaftCommandIndex": 9, "TaskId": 4}), False)

        self.assertIsInstance(command.result, AddQueueSinkOperationResult)
        self.assertEqual(9, command.result.raft_command_index)
        self.assertEqual(4, command.result.task_id)


class TestQueueSinkOngoingTask(unittest.TestCase):
    RESPONSE = {
        "TaskId": 4,
        "TaskType": "QueueSink",
        "TaskName": "orders-sink",
        "TaskState": "Enabled",
        "ResponsibleNode": {"NodeTag": "A", "NodeUrl": "http://localhost:8080"},
        "BrokerType": "Kafka",
        "ConnectionStringName": "kafka",
        "Url": "localhost:9092",
        "Configuration": {"Name": "orders-sink", "BrokerType": "Kafka", "Scripts": [{"Name": "orders"}]},
    }

    def test_ongoing_task_carries_the_broker_and_url(self):
        task = OngoingTaskQueueSink.from_json(self.RESPONSE)

        self.assertEqual(OngoingTaskType.QUEUE_SINK, task.task_type)
        self.assertEqual(QueueBrokerType.KAFKA, task.broker_type)
        self.assertEqual("localhost:9092", task.url)
        self.assertEqual("kafka", task.connection_string_name)

    def test_ongoing_task_parses_the_nested_configuration(self):
        task = OngoingTaskQueueSink.from_json(self.RESPONSE)

        self.assertIsInstance(task.configuration, QueueSinkConfiguration)
        self.assertEqual("orders", task.configuration.scripts[0].name)

    def test_get_ongoing_task_info_dispatches_to_the_queue_sink_result(self):
        command = GetOngoingTaskInfoOperation("orders-sink", OngoingTaskType.QUEUE_SINK).get_command(None)
        request = command.create_request(ServerNode("http://localhost:8080", "db"))
        command.set_response(json.dumps(self.RESPONSE), False)

        self.assertIn("type=QueueSink", request.url)
        self.assertIsInstance(command.result, OngoingTaskQueueSink)
        self.assertEqual("orders-sink", command.result.task_name)


class TestQueueSinkInDatabaseRecord(unittest.TestCase):
    def test_database_record_carries_queue_sinks(self):
        record = DatabaseRecord("db")
        record.queue_sinks = [_configuration()]

        self.assertEqual("orders-sink", record.to_json()["QueueSinks"][0]["Name"])

    def test_database_record_parses_queue_sinks(self):
        record = DatabaseRecord.from_json({"DatabaseName": "db", "QueueSinks": [_configuration().to_json()]})

        self.assertEqual(1, len(record.queue_sinks))
        self.assertEqual(QueueBrokerType.KAFKA, record.queue_sinks[0].broker_type)

    def test_a_record_from_a_server_without_queue_sinks_gets_an_empty_list(self):
        self.assertEqual([], DatabaseRecord.from_json({"DatabaseName": "db"}).queue_sinks)


class TestQueueSinkAgainstServer(TestBase):
    # Creating a queue sink task is licensed: an unlicensed server accepts it on a fresh
    # start but rejects it once the suite has built up databases and tasks, so the two
    # tests that create one are gated the way the rest of this repo gates licensed tests.

    def setUp(self):
        super().setUp()
        self.store.maintenance.send(
            PutConnectionStringOperation(
                QueueConnectionString(
                    name="kafka",
                    broker_type=QueueBrokerType.KAFKA,
                    kafka_settings=KafkaConnectionSettings(bootstrap_servers="localhost:9092"),
                )
            )
        )

    @unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
    def test_the_server_stores_a_queue_sink_task_and_reads_it_back(self):
        result = self.store.maintenance.send(AddQueueSinkOperation(_configuration()))
        self.assertGreater(result.task_id, 0)
        self.assertGreater(result.raft_command_index, 0)

        task = self.store.maintenance.send(GetOngoingTaskInfoOperation("orders-sink", OngoingTaskType.QUEUE_SINK))
        self.assertIsInstance(task, OngoingTaskQueueSink)
        self.assertEqual(QueueBrokerType.KAFKA, task.broker_type)
        # The server resolves the broker URL from the connection string.
        self.assertEqual("localhost:9092", task.url)
        self.assertEqual(["orders-topic"], task.configuration.scripts[0].queues)
        self.assertEqual("put('orders/', this);", task.configuration.scripts[0].script)

    @unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
    def test_the_task_can_be_updated(self):
        task_id = self.store.maintenance.send(AddQueueSinkOperation(_configuration())).task_id

        updated = _configuration()
        updated.task_id = task_id
        updated.scripts[0].queues = ["orders-topic", "returns-topic"]
        self.store.maintenance.send(UpdateQueueSinkOperation(task_id, updated))

        task = self.store.maintenance.send(GetOngoingTaskInfoOperation("orders-sink", OngoingTaskType.QUEUE_SINK))
        self.assertEqual(["orders-topic", "returns-topic"], task.configuration.scripts[0].queues)

    def test_a_configuration_with_no_scripts_is_refused_by_the_server(self):
        empty = QueueSinkConfiguration(
            name="empty-sink", broker_type=QueueBrokerType.KAFKA, connection_string_name="kafka"
        )

        self.assertRaisesWithMessageContaining(
            self.store.maintenance.send,
            RavenException,
            "'Scripts' list cannot be empty",
            AddQueueSinkOperation(empty),
        )
