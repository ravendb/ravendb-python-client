"""
Tests for the CDC Sink client surface added in 7.2.5: the configuration tree,
AddCdcSinkOperation / UpdateCdcSinkOperation, and the CdcSink ongoing task.
"""

import json
import unittest

from ravendb.documents.operations.cdc_sink import (
    AddCdcSinkOperation,
    AddCdcSinkOperationResult,
    CdcColumnMapping,
    CdcColumnType,
    CdcSinkConfiguration,
    CdcSinkEmbeddedTableConfig,
    CdcSinkLinkedTableConfig,
    CdcSinkOnDeleteConfig,
    CdcSinkPostgresSettings,
    CdcSinkProcessState,
    CdcSinkRelationType,
    CdcSinkTableConfig,
    CdcSinkTaskState,
    UpdateCdcSinkOperation,
)
from ravendb.documents.operations.ongoing_tasks import (
    GetOngoingTaskInfoOperation,
    OngoingTaskCdcSink,
    OngoingTaskType,
)
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.serverwide.database_record import DatabaseRecord


def _configuration() -> CdcSinkConfiguration:
    return CdcSinkConfiguration(
        name="orders-cdc",
        connection_string_name="pg",
        postgres=CdcSinkPostgresSettings(publication_name="pub", slot_name="slot"),
        skip_initial_load=True,
        tables=[
            CdcSinkTableConfig(
                collection_name="Orders",
                source_table_schema="public",
                source_table_name="orders",
                columns=[
                    CdcColumnMapping("id", "Id"),
                    CdcColumnMapping("payload", "Payload", CdcColumnType.JSON),
                    CdcColumnMapping("blob", "file.bin", CdcColumnType.ATTACHMENT),
                ],
                primary_key_columns=["id"],
                patch="this.Total = $row.total;",
                on_delete=CdcSinkOnDeleteConfig(patch="this.Archived = true;", ignore_deletes=True),
                embedded_tables=[
                    CdcSinkEmbeddedTableConfig(
                        source_table_name="order_lines",
                        property_name="Lines",
                        columns=[CdcColumnMapping("line_id", "LineId")],
                        primary_key_columns=["line_id"],
                        join_columns=["order_id"],
                        type_=CdcSinkRelationType.MAP,
                        case_sensitive_keys=True,
                        embedded_tables=[
                            CdcSinkEmbeddedTableConfig(
                                source_table_name="line_notes",
                                property_name="Notes",
                                columns=[CdcColumnMapping("note_id", "NoteId")],
                                primary_key_columns=["note_id"],
                                join_columns=["line_id"],
                            )
                        ],
                    )
                ],
                linked_tables=[
                    CdcSinkLinkedTableConfig(
                        source_table_name="customers",
                        property_name="Customer",
                        join_columns=["customer_id"],
                        linked_collection_name="Customers",
                    )
                ],
            )
        ],
    )


class TestCdcSinkConfiguration(unittest.TestCase):
    def test_configuration_survives_a_json_round_trip(self):
        configuration = _configuration()
        serialized = configuration.to_json()

        self.assertEqual(serialized, CdcSinkConfiguration.from_json(serialized).to_json())

    def test_configuration_serializes_the_task_level_fields(self):
        serialized = _configuration().to_json()

        self.assertEqual("orders-cdc", serialized["Name"])
        self.assertEqual("pg", serialized["ConnectionStringName"])
        self.assertTrue(serialized["SkipInitialLoad"])
        self.assertEqual({"PublicationName": "pub", "SlotName": "slot"}, serialized["Postgres"])
        self.assertEqual(0, serialized["TaskId"])
        self.assertFalse(serialized["Disabled"])

    def test_column_type_is_written_only_when_it_is_not_default(self):
        # The server reads a missing Type as Default, and so does the C# client.
        self.assertEqual({"Column": "c", "Name": "N"}, CdcColumnMapping("c", "N").to_json())
        self.assertEqual(
            {"Column": "c", "Name": "N", "Type": "Json"},
            CdcColumnMapping("c", "N", CdcColumnType.JSON).to_json(),
        )

    def test_column_without_a_type_deserializes_as_default(self):
        self.assertEqual(CdcColumnType.DEFAULT, CdcColumnMapping.from_json({"Column": "c", "Name": "N"}).type_)

    def test_embedded_tables_nest_to_any_depth(self):
        configuration = CdcSinkConfiguration.from_json(_configuration().to_json())
        lines = configuration.tables[0].embedded_tables[0]

        self.assertEqual(CdcSinkRelationType.MAP, lines.type_)
        self.assertTrue(lines.case_sensitive_keys)
        self.assertEqual("Notes", lines.embedded_tables[0].property_name)
        self.assertEqual(["line_id"], lines.embedded_tables[0].join_columns)

    def test_on_delete_round_trips(self):
        on_delete = CdcSinkConfiguration.from_json(_configuration().to_json()).tables[0].on_delete

        self.assertEqual("this.Archived = true;", on_delete.patch)
        self.assertTrue(on_delete.ignore_deletes)

    def test_a_table_without_an_on_delete_keeps_it_unset(self):
        table = CdcSinkTableConfig.from_json({"CollectionName": "Orders"})

        self.assertIsNone(table.on_delete)
        self.assertEqual([], table.columns)
        self.assertEqual([], table.embedded_tables)

    def test_collections_default_to_empty_rather_than_none(self):
        configuration = CdcSinkConfiguration()

        self.assertEqual([], configuration.tables)
        self.assertEqual([], CdcSinkEmbeddedTableConfig().columns)
        self.assertEqual([], CdcSinkLinkedTableConfig().join_columns)


class TestCdcSinkTaskState(unittest.TestCase):
    def test_task_state_round_trips(self):
        payload = {
            "ConfigurationName": "orders-cdc",
            "LastLsn": "0/16B3748",
            "Tables": {
                "public.orders": {
                    "InitialLoadCompleted": True,
                    "LastKeyValues": ["10"],
                    "KeyColumns": ["id"],
                }
            },
        }
        state = CdcSinkTaskState.from_json(payload)

        self.assertEqual("0/16B3748", state.last_lsn)
        self.assertTrue(state.tables["public.orders"].initial_load_completed)
        self.assertEqual(["id"], state.tables["public.orders"].key_columns)
        self.assertEqual(payload, state.to_json())

    def test_document_id_is_built_from_the_state_collection(self):
        self.assertEqual("@cdc-states", CdcSinkTaskState.COLLECTION_NAME)
        self.assertEqual("@cdc-states/orders-cdc", CdcSinkTaskState.get_document_id("orders-cdc"))

    def test_process_state_item_name(self):
        self.assertEqual("values/db/cdcsink/orders-cdc", CdcSinkProcessState.generate_item_name("db", "orders-cdc"))


class TestCdcSinkOperations(unittest.TestCase):
    def setUp(self):
        self.node = ServerNode("http://localhost:8080", "db")

    def test_add_sends_the_configuration_to_the_admin_endpoint(self):
        configuration = _configuration()
        command = AddCdcSinkOperation(configuration).get_command(None)
        request = command.create_request(self.node)

        self.assertEqual("PUT", request.method)
        self.assertEqual("http://localhost:8080/databases/db/admin/cdc-sink", request.url)
        self.assertEqual(configuration.to_json(), request.data)
        self.assertFalse(command.is_read_request())

    def test_update_addresses_the_task_by_id(self):
        command = UpdateCdcSinkOperation(42, _configuration()).get_command(None)
        request = command.create_request(self.node)

        self.assertEqual("PUT", request.method)
        self.assertEqual("http://localhost:8080/databases/db/admin/cdc-sink?id=42", request.url)

    def test_both_operations_are_raft_commands(self):
        # The server applies CDC Sink changes through Raft, so retries must reuse one id.
        add = AddCdcSinkOperation(_configuration()).get_command(None)
        update = UpdateCdcSinkOperation(1, _configuration()).get_command(None)

        self.assertIsInstance(add, RaftCommand)
        self.assertIsInstance(update, RaftCommand)
        self.assertTrue(add.get_raft_unique_request_id())

    def test_a_missing_configuration_is_rejected_client_side(self):
        with self.assertRaises(ValueError):
            AddCdcSinkOperation(None)
        with self.assertRaises(ValueError):
            UpdateCdcSinkOperation(1, None)

    def test_add_reads_the_task_id_off_the_response(self):
        command = AddCdcSinkOperation(_configuration()).get_command(None)
        command.set_response(json.dumps({"RaftCommandIndex": 17, "TaskId": 5}), False)

        self.assertIsInstance(command.result, AddCdcSinkOperationResult)
        self.assertEqual(17, command.result.raft_command_index)
        self.assertEqual(5, command.result.task_id)


class TestCdcSinkOngoingTask(unittest.TestCase):
    RESPONSE = {
        "TaskId": 7,
        "TaskType": "CdcSink",
        "TaskName": "orders-cdc",
        "TaskState": "Enabled",
        "ResponsibleNode": {"NodeTag": "A", "NodeUrl": "http://localhost:8080"},
        "ConnectionStringName": "pg",
        "FactoryName": "Npgsql",
        "LastCheckpoint": "0/16B3748",
        "LastBatchTime": "2026-06-16T10:30:00.0000000",
        "SecondsSinceLastBatch": 12.5,
        "LastActivityTime": "2026-06-16T10:30:05.0000000",
        "SecondsSinceLastActivity": 7.5,
        "HealthIssue": None,
        "Configuration": {"Name": "orders-cdc", "Tables": [{"CollectionName": "Orders"}]},
    }

    def test_cdc_sink_is_a_known_ongoing_task_type(self):
        self.assertEqual("CdcSink", OngoingTaskType.CDC_SINK.value)

    def test_ongoing_task_carries_the_health_and_lag_fields(self):
        task = OngoingTaskCdcSink.from_json(self.RESPONSE)

        self.assertEqual(OngoingTaskType.CDC_SINK, task.task_type)
        self.assertEqual("Npgsql", task.factory_name)
        self.assertEqual("0/16B3748", task.last_checkpoint)
        self.assertEqual(12.5, task.seconds_since_last_batch)
        self.assertEqual(7.5, task.seconds_since_last_activity)
        self.assertIsNone(task.health_issue)
        self.assertEqual("2026-06-16T10:30:00.0000000", task.to_json()["LastBatchTime"])

    def test_ongoing_task_parses_the_nested_configuration(self):
        task = OngoingTaskCdcSink.from_json(self.RESPONSE)

        self.assertIsInstance(task.configuration, CdcSinkConfiguration)
        self.assertEqual("Orders", task.configuration.tables[0].collection_name)

    def test_get_ongoing_task_info_dispatches_to_the_cdc_sink_result(self):
        command = GetOngoingTaskInfoOperation("orders-cdc", OngoingTaskType.CDC_SINK).get_command(None)
        request = command.create_request(ServerNode("http://localhost:8080", "db"))
        command.set_response(json.dumps(self.RESPONSE), False)

        self.assertIn("type=CdcSink", request.url)
        self.assertIsInstance(command.result, OngoingTaskCdcSink)
        self.assertEqual("orders-cdc", command.result.task_name)


class TestCdcSinkInDatabaseRecord(unittest.TestCase):
    def test_database_record_carries_cdc_sinks(self):
        record = DatabaseRecord("db")
        record.cdc_sinks = [_configuration()]

        self.assertEqual("orders-cdc", record.to_json()["CdcSinks"][0]["Name"])

    def test_database_record_parses_cdc_sinks(self):
        record = DatabaseRecord.from_json(
            {"DatabaseName": "db", "LockMode": "Unlock", "AutoIndexes": {}, "CdcSinks": [_configuration().to_json()]}
        )

        self.assertEqual(1, len(record.cdc_sinks))
        self.assertEqual("Orders", record.cdc_sinks[0].tables[0].collection_name)

    def test_a_record_from_a_server_without_cdc_sinks_gets_an_empty_list(self):
        record = DatabaseRecord.from_json({"DatabaseName": "db", "LockMode": "Unlock", "AutoIndexes": {}})

        self.assertEqual([], record.cdc_sinks)
