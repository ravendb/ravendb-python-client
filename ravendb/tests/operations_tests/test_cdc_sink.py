"""CDC Sink client API tests, ported from CdcSinkCrudTests.

The CRUD class needs a licensed server: CDC Sink is a licensed feature.
"""

import json
import os
import unittest
from datetime import datetime

from ravendb.exceptions.raven_exceptions import RavenException
from ravendb.documents.operations.etl.sql import SqlConnectionString
from ravendb.documents.operations.connection_string.put_connection_string_operation import (
    PutConnectionStringOperation,
)
from ravendb.documents.operations.ongoing_tasks import (
    DeleteOngoingTaskOperation,
    GetOngoingTaskInfoOperation,
    OngoingTaskCdcSink,
    OngoingTaskType,
    ToggleOngoingTaskStateOperation,
)
from ravendb.documents.operations.cdc_sink import (
    AddCdcSinkOperation,
    CdcColumnMapping,
    CdcColumnType,
    CdcSinkConfiguration,
    CdcSinkEmbeddedTableConfig,
    CdcSinkLinkedTableConfig,
    CdcSinkOnDeleteConfig,
    CdcSinkPostgresSettings,
    CdcSinkRelationType,
    CdcSinkTableConfig,
    UpdateCdcSinkOperation,
)
from ravendb.http.server_node import ServerNode
from ravendb.tests.test_base import TestBase


def _build_config(name, connection_string_name):
    return CdcSinkConfiguration(
        name=name,
        connection_string_name=connection_string_name,
        tables=[
            CdcSinkTableConfig(
                collection_name="Orders",
                source_table_schema="dbo",
                source_table_name="orders",
                columns=[
                    CdcColumnMapping(column="order_id", name="OrderId"),
                    CdcColumnMapping(column="customer_id", name="CustomerId"),
                ],
                primary_key_columns=["order_id"],
            )
        ],
    )


class TestCdcSinkWireShape(unittest.TestCase):
    """Serialization shapes, pinned to the reference ToJson payloads."""

    def test_configuration_to_json_key_set(self):
        config = _build_config("test-cdc", "sql-cs")
        payload = config.to_json()
        self.assertEqual(
            set(payload.keys()),
            {
                "Name",
                "TaskId",
                "Disabled",
                "ConnectionStringName",
                "MentorNode",
                "PinToMentorNode",
                "Tables",
                "Postgres",
                "SkipInitialLoad",
            },
        )

    def test_table_config_to_json_key_set(self):
        table = _build_config("c", "cs").tables[0]
        self.assertEqual(
            set(table.to_json().keys()),
            {
                "CollectionName",
                "SourceTableSchema",
                "SourceTableName",
                "Columns",
                "PrimaryKeyColumns",
                "Patch",
                "OnDelete",
                "Disabled",
                "EmbeddedTables",
                "LinkedTables",
            },
        )

    def test_column_mapping_type_omitted_when_default(self):
        mapping = CdcColumnMapping(column="order_id", name="OrderId")
        self.assertEqual(mapping.to_json(), {"Column": "order_id", "Name": "OrderId"})
        mapping.type = CdcColumnType.JSON
        self.assertEqual(mapping.to_json(), {"Column": "order_id", "Name": "OrderId", "Type": "Json"})
        mapping.type = CdcColumnType.ATTACHMENT
        self.assertEqual(
            mapping.to_json(),
            {"Column": "order_id", "Name": "OrderId", "Type": "Attachment"},
        )

    def test_postgres_settings_to_json(self):
        postgres = CdcSinkPostgresSettings(publication_name="pub", slot_name="slot")
        self.assertEqual(postgres.to_json(), {"PublicationName": "pub", "SlotName": "slot"})

    def test_embedded_table_config_to_json_writes_type_always(self):
        embedded = CdcSinkEmbeddedTableConfig(
            source_table_name="order_items",
            property_name="Items",
            join_columns=["order_id"],
            primary_key_columns=["id"],
            columns=[CdcColumnMapping(column="id", name="Id")],
        )
        payload = embedded.to_json()
        self.assertEqual(
            set(payload.keys()),
            {
                "SourceTableSchema",
                "SourceTableName",
                "PropertyName",
                "Columns",
                "PrimaryKeyColumns",
                "JoinColumns",
                "Type",
                "Patch",
                "OnDelete",
                "CaseSensitiveKeys",
                "EmbeddedTables",
                "LinkedTables",
            },
        )
        # Unlike CdcColumnMapping, Type is always written, and the default
        # enum member (Array) is written as its name.
        self.assertEqual("Array", payload["Type"])
        embedded.type = CdcSinkRelationType.MAP
        self.assertEqual("Map", embedded.to_json()["Type"])
        embedded.type = CdcSinkRelationType.VALUE
        self.assertEqual("Value", embedded.to_json()["Type"])

    def test_linked_table_config_to_json(self):
        linked = CdcSinkLinkedTableConfig(
            source_table_name="customers",
            property_name="Customer",
            join_columns=["customer_id"],
            linked_collection_name="Customers",
        )
        self.assertEqual(
            linked.to_json(),
            {
                "SourceTableSchema": None,
                "SourceTableName": "customers",
                "PropertyName": "Customer",
                "JoinColumns": ["customer_id"],
                "LinkedCollectionName": "Customers",
            },
        )

    def test_on_delete_config_to_json(self):
        on_delete = CdcSinkOnDeleteConfig(patch="this.Archived = true;", ignore_deletes=True)
        self.assertEqual(
            on_delete.to_json(),
            {"Patch": "this.Archived = true;", "IgnoreDeletes": True},
        )

    def test_configuration_from_json_round_trip(self):
        payload = _build_config("test-cdc", "sql-cs").to_json()
        payload["Postgres"] = {"PublicationName": "pub", "SlotName": "slot"}
        config = CdcSinkConfiguration.from_json(payload)
        self.assertEqual("test-cdc", config.name)
        self.assertEqual("sql-cs", config.connection_string_name)
        self.assertEqual(1, len(config.tables))
        table = config.tables[0]
        self.assertEqual("Orders", table.collection_name)
        self.assertEqual("dbo", table.source_table_schema)
        self.assertEqual("orders", table.source_table_name)
        self.assertEqual(["order_id"], table.primary_key_columns)
        self.assertEqual("order_id", table.columns[0].column)
        self.assertEqual("OrderId", table.columns[0].name)
        self.assertEqual("pub", config.postgres.publication_name)
        self.assertEqual("slot", config.postgres.slot_name)


class TestCdcSinkOperationWireShape(unittest.TestCase):
    """Add/update URLs and response parsing, pinned to the reference operations."""

    def _body(self, request):
        return json.loads(request.data) if isinstance(request.data, str) else request.data

    def test_add_operation_url_and_body(self):
        config = _build_config("test-cdc", "sql-cs")
        command = AddCdcSinkOperation(config).get_command(None)
        request = command.create_request(ServerNode("http://localhost:8080", "db"))
        self.assertEqual("PUT", request.method)
        self.assertEqual("http://localhost:8080/databases/db/admin/cdc-sink", request.url)
        self.assertEqual(config.to_json(), self._body(request))

    def test_update_operation_url_and_body(self):
        config = _build_config("test-cdc", "sql-cs")
        command = UpdateCdcSinkOperation(42, config).get_command(None)
        request = command.create_request(ServerNode("http://localhost:8080", "db"))
        self.assertEqual("PUT", request.method)
        self.assertEqual("http://localhost:8080/databases/db/admin/cdc-sink?id=42", request.url)
        self.assertEqual(config.to_json(), self._body(request))

    def test_add_result_parses_raft_command_index_and_task_id(self):
        command = AddCdcSinkOperation(_build_config("c", "cs")).get_command(None)
        command.set_response('{"RaftCommandIndex": 7, "TaskId": 7}', False)
        self.assertEqual(7, command.result.raft_command_index)
        self.assertEqual(7, command.result.task_id)

    def test_update_result_parses_raft_command_index_and_task_id(self):
        command = UpdateCdcSinkOperation(7, _build_config("c", "cs")).get_command(None)
        command.set_response('{"RaftCommandIndex": 9, "TaskId": 9}', False)
        self.assertEqual(9, command.result.raft_command_index)
        self.assertEqual(9, command.result.task_id)


class TestCdcSinkTaskInfoWireShape(unittest.TestCase):
    """The /task?type=CdcSink response shape and the OngoingTaskCdcSink parse."""

    _CONFIGURATION = {
        "Name": "test-cdc",
        "TaskId": 42,
        "Disabled": False,
        "ConnectionStringName": "sql-cs",
        "MentorNode": None,
        "PinToMentorNode": False,
        "Tables": [
            {
                "CollectionName": "Orders",
                "SourceTableSchema": "dbo",
                "SourceTableName": "orders",
                "Columns": [{"Column": "order_id", "Name": "OrderId"}],
                "PrimaryKeyColumns": ["order_id"],
                "Patch": None,
                "OnDelete": None,
                "Disabled": False,
                "EmbeddedTables": [],
                "LinkedTables": [],
            }
        ],
        "Postgres": None,
        "SkipInitialLoad": False,
    }

    _TASK_INFO = {
        "TaskId": 42,
        "TaskName": "test-cdc",
        "TaskType": "CdcSink",
        "TaskState": "Enabled",
        "TaskConnectionStatus": "Active",
        "ResponsibleNode": {"NodeTag": "A", "NodeUrl": "http://127.0.0.1:8080"},
        "Error": None,
        "MentorNode": None,
        "PinToMentorNode": False,
        "Configuration": _CONFIGURATION,
        "ConnectionStringName": "sql-cs",
        "FactoryName": "Microsoft.Data.SqlClient",
        "LastBatchTime": "2026-06-01T12:00:00.0000000",
        "LastCheckpoint": "0/1A2B3C",
        "SecondsSinceLastBatch": 12.5,
        "LastActivityTime": "2026-06-01T12:00:30.0000000",
        "SecondsSinceLastActivity": 3.25,
        "HealthIssue": None,
    }

    def _get_task_command(self):
        operation = GetOngoingTaskInfoOperation(42, OngoingTaskType.CDC_SINK)
        return operation.get_command(None)

    def test_get_task_info_url_uses_type_cdc_sink(self):
        request = self._get_task_command().create_request(ServerNode("http://localhost:8080", "db"))
        self.assertEqual("http://localhost:8080/databases/db/task?key=42&type=CdcSink", request.url)

    def test_get_task_info_dispatches_to_ongoing_task_cdc_sink(self):
        command = self._get_task_command()
        command.set_response(json.dumps(self._TASK_INFO), False)
        task = command.result
        self.assertIsInstance(task, OngoingTaskCdcSink)
        self.assertEqual(OngoingTaskType.CDC_SINK, task.task_type)

    def test_task_info_from_json_binds_all_fields(self):
        task = OngoingTaskCdcSink.from_json(self._TASK_INFO)
        self.assertEqual(42, task.task_id)
        self.assertEqual("test-cdc", task.task_name)
        self.assertEqual("sql-cs", task.connection_string_name)
        self.assertEqual("Microsoft.Data.SqlClient", task.factory_name)
        self.assertEqual("0/1A2B3C", task.last_checkpoint)
        self.assertEqual(12.5, task.seconds_since_last_batch)
        self.assertEqual(3.25, task.seconds_since_last_activity)
        self.assertEqual(datetime(2026, 6, 1, 12, 0, 0), task.last_batch_time)
        self.assertEqual(datetime(2026, 6, 1, 12, 0, 30), task.last_activity_time)
        self.assertIsNone(task.health_issue)
        self.assertEqual("test-cdc", task.configuration.name)
        self.assertEqual("Orders", task.configuration.tables[0].collection_name)
        self.assertEqual("order_id", task.configuration.tables[0].columns[0].column)

    def test_task_info_absent_runtime_fields_are_none(self):
        task = OngoingTaskCdcSink.from_json(
            {"TaskId": 42, "TaskName": "test-cdc", "Configuration": self._CONFIGURATION}
        )
        self.assertIsNone(task.last_checkpoint)
        self.assertIsNone(task.seconds_since_last_batch)
        self.assertIsNone(task.last_batch_time)
        self.assertIsNone(task.health_issue)
        self.assertIsNone(task.factory_name)


class TestCdcSinkValidate(unittest.TestCase):
    """validate() error strings, pinned to CdcSinkConfiguration.Validate."""

    def _config(self, **kwargs):
        config = _build_config("test-cdc", "sql-cs")
        for key, value in kwargs.items():
            setattr(config, key, value)
        return config

    def test_empty_name(self):
        errors = self._config(name="").validate()
        self.assertEqual(["Name of CDC Sink configuration cannot be empty"], errors)

    def test_empty_connection_string_name(self):
        errors = self._config(connection_string_name="").validate()
        self.assertEqual(["ConnectionStringName cannot be empty"], errors)

    def test_empty_tables(self):
        errors = self._config(tables=[]).validate()
        self.assertEqual(["'Tables' list cannot be empty."], errors)

    def test_table_checks(self):
        table = CdcSinkTableConfig(
            collection_name="Orders",
            source_table_name="orders",
            columns=[],
            primary_key_columns=[],
        )
        errors = self._config(tables=[table]).validate()
        self.assertIn("Table 'Orders' must have at least one primary key column", errors)
        self.assertIn("Table 'Orders' must have at least one column mapping", errors)

        table.collection_name = ""
        table.source_table_name = ""
        table.columns = [CdcColumnMapping(column="order_id", name="OrderId")]
        table.primary_key_columns = ["order_id"]
        errors = self._config(tables=[table]).validate()
        self.assertIn("Table collection name must not be empty", errors)
        self.assertIn("Table '' must have a source table name", errors)

    def test_duplicate_table_name_case_insensitive(self):
        second = _build_config("c", "cs").tables[0]
        second.collection_name = "orders"
        errors = self._config(tables=[_build_config("c", "cs").tables[0], second]).validate()
        self.assertIn("Table name 'orders' is already defined. Table names must be unique", errors)

    def test_primary_key_not_in_column_mappings(self):
        table = _build_config("c", "cs").tables[0]
        table.primary_key_columns = ["missing_pk"]
        errors = self._config(tables=[table]).validate()
        self.assertIn(
            "Table 'Orders': primary key column 'missing_pk' is not listed in the column mappings. "
            "Primary key columns must be included in the column mappings so they are stored in the "
            "document — without them, the system cannot identify which array element to update or "
            "delete on subsequent changes. Add a column mapping for this column "
            '(e.g. { Column = "missing_pk", Name = "..." }) or correct the primary key column name.',
            errors,
        )

    def test_column_mapping_errors(self):
        table = _build_config("c", "cs").tables[0]
        table.columns = [
            CdcColumnMapping(column="", name="OrderId"),
            CdcColumnMapping(column="customer_id", name=""),
        ]
        errors = self._config(tables=[table]).validate()
        self.assertIn(
            "Table 'Orders': column mapping has an empty Column name (Name: 'OrderId')",
            errors,
        )
        self.assertIn("Table 'Orders': column 'customer_id' has an empty Name", errors)

        table.columns = [
            CdcColumnMapping(column="order_id", name="OrderId"),
            CdcColumnMapping(column="order_id", name="Other"),
        ]
        errors = self._config(tables=[table]).validate()
        self.assertIn("Table 'Orders': duplicate column 'order_id'", errors)

        table.columns = [
            CdcColumnMapping(column="order_id", name="OrderId"),
            CdcColumnMapping(column="customer_id", name="OrderId"),
        ]
        errors = self._config(tables=[table]).validate()
        self.assertIn(
            "Table 'Orders': duplicate target name 'OrderId' (used by multiple columns)",
            errors,
        )

    def test_embedded_table_errors(self):
        table = _build_config("c", "cs").tables[0]
        table.embedded_tables = [
            CdcSinkEmbeddedTableConfig(
                source_table_name="order_items",
                property_name="Items",
                columns=[],
                primary_key_columns=[],
                join_columns=[],
            )
        ]
        errors = self._config(tables=[table]).validate()
        self.assertIn("Embedded table 'order_items' under 'Orders' must have join columns", errors)
        self.assertIn(
            "Embedded table 'order_items' under 'Orders' must have primary key columns",
            errors,
        )
        self.assertIn(
            "Embedded table 'order_items' under 'Orders' must have at least one column mapping",
            errors,
        )

        embedded = CdcSinkEmbeddedTableConfig(
            source_table_name="orders",
            property_name="Items",
            columns=[CdcColumnMapping(column="id", name="Id")],
            primary_key_columns=["id"],
            join_columns=["order_id"],
        )
        table.embedded_tables = [embedded]
        errors = self._config(tables=[table]).validate()
        self.assertIn(
            "Embedded table 'orders' under 'Orders' cannot reference its own parent table",
            errors,
        )

        embedded = CdcSinkEmbeddedTableConfig(
            source_table_name="order_items",
            property_name="",
            columns=[CdcColumnMapping(column="id", name="Id")],
            primary_key_columns=["id"],
            join_columns=["order_id"],
        )
        table.embedded_tables = [embedded]
        errors = self._config(tables=[table]).validate()
        self.assertIn(
            "Embedded table 'order_items' under 'Orders' must have a property name",
            errors,
        )

    def test_linked_table_errors(self):
        table = _build_config("c", "cs").tables[0]
        table.linked_tables = [
            CdcSinkLinkedTableConfig(
                source_table_name="",
                property_name="",
                linked_collection_name="",
                join_columns=[],
            )
        ]
        errors = self._config(tables=[table]).validate()
        self.assertIn("Linked table under 'Orders' must have a source table name", errors)
        self.assertIn("Linked table '' under 'Orders' must have a property name", errors)
        self.assertIn("Linked table '' under 'Orders' must have a linked collection name", errors)
        self.assertIn("Linked table '' under 'Orders' must have join columns", errors)

    def test_property_name_conflict_with_column_mapping(self):
        table = _build_config("c", "cs").tables[0]
        table.embedded_tables = [
            CdcSinkEmbeddedTableConfig(
                source_table_name="order_items",
                property_name="OrderId",
                columns=[CdcColumnMapping(column="id", name="Id")],
                primary_key_columns=["id"],
                join_columns=["order_id"],
            )
        ]
        errors = self._config(tables=[table]).validate()
        self.assertIn(
            "Table 'Orders': property name 'OrderId' from embedded table 'order_items' conflicts with "
            "a column mapping or another embedded/linked table",
            errors,
        )


@unittest.skipIf(
    os.environ.get("RAVENDB_LICENSE") is None and os.environ.get("RAVEN_License") is None,
    "Insufficient license permissions. Skipping on CI/CD.",
)
class TestCdcSinkCrud(TestBase):
    # Ported from CdcSinkCrudTests.cs: CanAddCdcSinkTask,
    # CanUpdateCdcSinkTask, CanDeleteCdcSinkTask, CanGetCdcSinkTaskInfo,
    # CanToggleCdcSinkTaskState, CanAddMultipleCdcSinkTasks.

    def setUp(self):
        super().setUp()
        self.connection_string_name = "sql-cs"
        self.store.maintenance.send(
            PutConnectionStringOperation(
                SqlConnectionString(
                    name=self.connection_string_name,
                    factory_name="Microsoft.Data.SqlClient",
                    connection_string="Server=localhost;Database=test;",
                )
            )
        )

    def _send(self, operation):
        try:
            return self.store.maintenance.send(operation)
        except RavenException as e:
            # The CDC sink license feature gates the endpoint; the limit is
            # asserted from the first license-status refresh onwards, so a
            # freshly started server may accept the command before refusing.
            if "CDC sink feature" in str(e):
                self.skipTest("License does not support the CDC sink feature")
            raise

    def _get_task(self, task_id):
        return self._send(GetOngoingTaskInfoOperation(task_id, OngoingTaskType.CDC_SINK))

    def test_add_cdc_sink_task(self):
        config = _build_config("test-cdc", self.connection_string_name)
        result = self._send(AddCdcSinkOperation(config))
        self.assertIsNotNone(result)
        self.assertGreater(result.task_id, 0)
        self.assertIsNotNone(result.raft_command_index)

        task = self._get_task(result.task_id)
        self.assertIsNotNone(task)
        self.assertEqual(task.task_type, OngoingTaskType.CDC_SINK)
        self.assertEqual(task.configuration.name, "test-cdc")
        self.assertEqual(task.connection_string_name, self.connection_string_name)

    def test_update_cdc_sink_task(self):
        config = _build_config("test-cdc", self.connection_string_name)
        add_result = self._send(AddCdcSinkOperation(config))

        config.tables[0].source_table_name = "updated_orders"
        self._send(UpdateCdcSinkOperation(add_result.task_id, config))

        # The server applies an update as delete + re-add, so the task gets a
        # new TaskId; task ids are not stable across updates, names are.
        task = self._send(GetOngoingTaskInfoOperation("test-cdc", OngoingTaskType.CDC_SINK))
        self.assertIsNotNone(task)
        self.assertEqual(task.configuration.tables[0].source_table_name, "updated_orders")

    def test_delete_cdc_sink_task(self):
        config = _build_config("test-cdc", self.connection_string_name)
        add_result = self._send(AddCdcSinkOperation(config))
        self._send(DeleteOngoingTaskOperation(add_result.task_id, OngoingTaskType.CDC_SINK))
        task = self._get_task(add_result.task_id)
        self.assertIsNone(task)

    def test_toggle_cdc_sink_task_state(self):
        config = _build_config("test-cdc", self.connection_string_name)
        add_result = self._send(AddCdcSinkOperation(config))
        task_id = add_result.task_id

        self._send(ToggleOngoingTaskStateOperation(task_id, OngoingTaskType.CDC_SINK, disable=True))
        self.assertTrue(self._get_task(task_id).configuration.disabled)

        self._send(ToggleOngoingTaskStateOperation(task_id, OngoingTaskType.CDC_SINK, disable=False))
        self.assertFalse(self._get_task(task_id).configuration.disabled)

    def test_add_multiple_cdc_sink_tasks(self):
        for name in ("cdc-sink-1", "cdc-sink-2"):
            result = self._send(AddCdcSinkOperation(_build_config(name, self.connection_string_name)))
            self.assertGreater(result.task_id, 0)
            task = self._get_task(result.task_id)
            self.assertEqual(task.configuration.name, name)
