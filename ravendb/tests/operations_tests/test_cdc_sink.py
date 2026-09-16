"""
Tests for the CDC Sink client surface added in 7.2.5: the configuration tree,
AddCdcSinkOperation / UpdateCdcSinkOperation, and the CdcSink ongoing task.
"""

import json
import os
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
from ravendb.documents.operations.cdc_sink.schema import (
    CdcSinkSourceSchema,
    GetCdcSinkSchemaOperation,
)
from ravendb.documents.operations.cdc_sink.testing import (
    TestCdcSinkMappingOperation,
    TestCdcSinkMappingRequest,
    TestCdcSinkMappingResult,
    TestCdcSinkOperation,
    TestCdcSinkRowSelector,
)
from ravendb.documents.operations.connection_string.put_connection_string_operation import (
    PutConnectionStringOperation,
)
from ravendb.documents.operations.etl.sql import SqlConnectionString
from ravendb.http.server_node import ServerNode
from ravendb.tests.test_base import TestBase
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


class TestCdcSinkSchemaDiscovery(unittest.TestCase):
    RESPONSE = {
        "CatalogName": "northwind",
        "HasPermissionToSetup": True,
        "Warnings": ["SQL Server Agent is not running."],
        "Errors": [],
        "Tables": [
            {
                "SourceTableSchema": "public",
                "SourceTableName": "orders",
                "IsCdcEnabled": True,
                "PrimaryKeyColumns": ["id"],
                "Warnings": ["REPLICA IDENTITY will not carry row-identifying columns on DELETE."],
                "Columns": [
                    {
                        "Name": "id",
                        "NativeType": "bigint",
                        "SuggestedType": "Default",
                        "IsPrimaryKey": True,
                        "IsCdcCapturable": True,
                    },
                    {"Name": "payload", "NativeType": "jsonb", "SuggestedType": "Json", "IsCdcCapturable": True},
                    {"Name": "blob", "NativeType": "bytea", "SuggestedType": "Attachment", "IsCdcCapturable": True},
                    {
                        "Name": "weird",
                        "NativeType": "cube",
                        "IsCdcCapturable": False,
                        "UnsupportedReason": "No CDC mapping for this type.",
                    },
                ],
                "ForeignKeys": [
                    {
                        "Columns": ["customer_id"],
                        "ReferencedSchema": "public",
                        "ReferencedTable": "customers",
                        "ReferencedColumns": ["id"],
                    }
                ],
            }
        ],
    }

    def test_schema_response_is_parsed(self):
        schema = CdcSinkSourceSchema.from_json(self.RESPONSE)

        self.assertEqual("northwind", schema.catalog_name)
        self.assertTrue(schema.has_permission_to_setup)
        self.assertEqual(1, len(schema.tables))
        self.assertEqual(["SQL Server Agent is not running."], schema.warnings)

    def test_success_follows_errors_not_warnings(self):
        self.assertTrue(CdcSinkSourceSchema.from_json(self.RESPONSE).success)
        self.assertFalse(CdcSinkSourceSchema.from_json({"Errors": ["boom"]}).success)
        # A warning is advisory, so it must not flip success.
        self.assertTrue(CdcSinkSourceSchema.from_json({"Warnings": ["heads up"]}).success)

    def test_columns_carry_their_suggested_mapping(self):
        columns = CdcSinkSourceSchema.from_json(self.RESPONSE).tables[0].columns

        self.assertEqual(CdcColumnType.DEFAULT, columns[0].suggested_type)
        self.assertTrue(columns[0].is_primary_key)
        self.assertEqual(CdcColumnType.JSON, columns[1].suggested_type)
        self.assertEqual(CdcColumnType.ATTACHMENT, columns[2].suggested_type)

    def test_an_uncapturable_column_says_why(self):
        column = CdcSinkSourceSchema.from_json(self.RESPONSE).tables[0].columns[3]

        self.assertFalse(column.is_cdc_capturable)
        self.assertEqual("No CDC mapping for this type.", column.unsupported_reason)

    def test_foreign_keys_are_parsed(self):
        foreign_key = CdcSinkSourceSchema.from_json(self.RESPONSE).tables[0].foreign_keys[0]

        self.assertEqual(["customer_id"], foreign_key.columns)
        self.assertEqual("customers", foreign_key.referenced_table)
        self.assertEqual(["id"], foreign_key.referenced_columns)

    def test_schema_round_trips(self):
        schema = CdcSinkSourceSchema.from_json(self.RESPONSE)

        self.assertEqual(schema.to_json(), CdcSinkSourceSchema.from_json(schema.to_json()).to_json())

    def test_an_empty_response_parses(self):
        schema = CdcSinkSourceSchema.from_json({})

        self.assertEqual([], schema.tables)
        self.assertTrue(schema.success)

    def test_the_operation_takes_a_connection_or_a_name(self):
        node = ServerNode("http://localhost:8080", "db")
        connection = SqlConnectionString("pg", "Host=localhost", "Npgsql")

        by_connection = GetCdcSinkSchemaOperation(connection, ["public"]).get_command(None)
        request = by_connection.create_request(node)
        self.assertEqual("POST", request.method)
        self.assertEqual("http://localhost:8080/databases/db/admin/cdc-sink/schema", request.url)
        self.assertEqual("pg", request.data["Connection"]["Name"])
        self.assertEqual(["public"], request.data["Schemas"])
        self.assertIsNone(request.data["ConnectionStringName"])

        by_name = GetCdcSinkSchemaOperation("pg").get_command(None)
        self.assertEqual("pg", by_name.create_request(node).data["ConnectionStringName"])

    def test_the_operation_allows_fastest_node_failover(self):
        # A POST, but nothing changes server-side.
        self.assertTrue(GetCdcSinkSchemaOperation("pg").get_command(None).is_read_request())

    def test_the_operation_needs_a_connection_or_a_name(self):
        with self.assertRaises(ValueError):
            GetCdcSinkSchemaOperation()
        with self.assertRaises(ValueError):
            GetCdcSinkSchemaOperation(42)


class TestCdcSinkMappingPreview(unittest.TestCase):
    def setUp(self):
        self.node = ServerNode("http://localhost:8080", "db")
        self.request = TestCdcSinkMappingRequest(
            configuration=_configuration(),
            connection=SqlConnectionString("pg", "Host=localhost", "Npgsql"),
            source_table_schema="public",
            source_table_name="orders",
            max_rows=3,
        )

    def test_the_request_carries_the_configuration_and_the_row_choice(self):
        body = self.request.to_json()

        self.assertEqual("orders", body["SourceTableName"])
        self.assertEqual("First", body["RowSelector"])
        self.assertEqual("Upsert", body["Operation"])
        self.assertEqual(3, body["MaxRows"])
        self.assertEqual("orders-cdc", body["Configuration"]["Name"])

    def test_a_by_primary_key_request_names_the_key_values(self):
        request = TestCdcSinkMappingRequest(
            configuration=_configuration(),
            row_selector=TestCdcSinkRowSelector.BY_PRIMARY_KEY,
            primary_key_values=["42"],
            operation=TestCdcSinkOperation.DELETE,
        )
        body = request.to_json()

        self.assertEqual("ByPrimaryKey", body["RowSelector"])
        self.assertEqual(["42"], body["PrimaryKeyValues"])
        self.assertEqual("Delete", body["Operation"])

    def test_the_operation_posts_to_the_test_endpoint(self):
        command = TestCdcSinkMappingOperation(self.request).get_command(None)
        request = command.create_request(self.node)

        self.assertEqual("POST", request.method)
        self.assertEqual("http://localhost:8080/databases/db/admin/cdc-sink/test", request.url)
        self.assertTrue(command.is_read_request())

    def test_a_missing_request_is_rejected_client_side(self):
        with self.assertRaises(ValueError):
            TestCdcSinkMappingOperation(None)

    def test_row_results_are_parsed(self):
        command = TestCdcSinkMappingOperation(self.request).get_command(None)
        command.set_response(
            json.dumps(
                {
                    "Results": [
                        {
                            "DocumentId": "orders/42",
                            "Document": '{"Id":42}',
                            "SourceRow": '{"id":42}',
                            "WouldDelete": False,
                            "IgnoreDeletes": False,
                            "DebugOutput": ["mapped"],
                        },
                        {"DocumentId": "orders/43", "Error": "the patch threw"},
                    ],
                    "Errors": [],
                    "Warnings": ["Linked tables are not exercised in test mode."],
                }
            ),
            False,
        )
        result = command.result

        self.assertIsInstance(result, TestCdcSinkMappingResult)
        self.assertEqual(2, len(result.results))
        self.assertEqual("orders/42", result.results[0].document_id)
        self.assertEqual(["mapped"], result.results[0].debug_output)
        # A row that failed on its own carries the error; the request still succeeded.
        self.assertEqual("the patch threw", result.results[1].error)
        self.assertEqual([], result.errors)
        self.assertEqual(1, len(result.warnings))

    def test_a_whole_request_failure_leaves_no_rows(self):
        result = TestCdcSinkMappingResult.from_json({"Errors": ["Cannot open connection"]})

        self.assertEqual([], result.results)
        self.assertEqual(["Cannot open connection"], result.errors)

    def test_result_round_trips(self):
        result = TestCdcSinkMappingResult.from_json(
            {"Results": [{"DocumentId": "orders/1", "WouldDelete": True}], "Errors": [], "Warnings": []}
        )

        self.assertEqual(result.to_json(), TestCdcSinkMappingResult.from_json(result.to_json()).to_json())


@unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
class TestCdcSinkAgainstServer(TestBase):
    # CDC Sink is licensed, and each of these needs the server to accept the payload
    # before it ever reaches a source database.

    def test_the_server_answers_schema_discovery_with_a_structured_result(self):
        # There is no PostgreSQL to reach, so what matters is that the server parsed the
        # request and answered in the shape the client expects instead of failing.
        schema = self.store.maintenance.send(GetCdcSinkSchemaOperation("no-such-connection-string"))

        self.assertIsInstance(schema, CdcSinkSourceSchema)
        self.assertFalse(schema.success)
        self.assertTrue(any("no-such-connection-string" in error for error in schema.errors))

    def test_the_server_reads_a_full_configuration_out_of_a_mapping_preview(self):
        request = TestCdcSinkMappingRequest(
            configuration=_configuration(),
            connection=SqlConnectionString("pg", "Host=localhost;Database=nope", "Npgsql"),
            source_table_schema="public",
            source_table_name="orders",
        )

        result = self.store.maintenance.send(TestCdcSinkMappingOperation(request))

        self.assertIsInstance(result, TestCdcSinkMappingResult)
        # It reached the driver, which means the whole configuration tree deserialized.
        self.assertTrue(any("source database" in error for error in result.errors))

    def test_a_cdc_sink_task_is_stored_and_read_back(self):
        self.store.maintenance.send(
            PutConnectionStringOperation(SqlConnectionString("pg", "Host=localhost;Database=nope", "Npgsql"))
        )

        result = self.store.maintenance.send(AddCdcSinkOperation(_configuration()))
        self.assertGreater(result.task_id, 0)

        task = self.store.maintenance.send(GetOngoingTaskInfoOperation("orders-cdc", OngoingTaskType.CDC_SINK))
        self.assertIsInstance(task, OngoingTaskCdcSink)
        self.assertEqual("pg", task.connection_string_name)
        self.assertEqual("Orders", task.configuration.tables[0].collection_name)
        self.assertEqual(
            ["Lines"], [embedded.property_name for embedded in task.configuration.tables[0].embedded_tables]
        )
