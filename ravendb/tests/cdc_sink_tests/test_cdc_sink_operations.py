"""Wire and dispatch tests for the CDC Sink operations: add/update request
shapes and results, server-error surfacing, ongoing-task integration, and
the DatabaseRecord.CdcSinks list.
"""

import json
import unittest

from ravendb.documents.operations.cdc_sink.add_cdc_sink_operation import (
    AddCdcSinkCommand,
    AddCdcSinkOperation,
    AddCdcSinkOperationResult,
)
from ravendb.documents.operations.cdc_sink.cdc_sink_configuration import CdcSinkConfiguration
from ravendb.documents.operations.cdc_sink.update_cdc_sink_operation import (
    UpdateCdcSinkCommand,
    UpdateCdcSinkOperation,
    UpdateCdcSinkOperationResult,
)
from ravendb.documents.operations.ongoing_tasks import (
    DeleteOngoingTaskOperation,
    GetOngoingTaskInfoOperation,
    OngoingTask,
    OngoingTaskCdcSink,
    OngoingTaskType,
)
from ravendb.exceptions.exception_dispatcher import ExceptionDispatcher
from ravendb.exceptions.raven_exceptions import RavenException
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.serverwide.database_record import DatabaseRecord


class TestAddCdcSinkOperationWire(unittest.TestCase):
    def setUp(self):
        self.node = ServerNode("http://localhost:8080", "db1")
        self.config = CdcSinkConfiguration(name="cdc-1", connection_string_name="sql-cs")
        self.command = AddCdcSinkCommand(None, self.config)

    def test_put_to_admin_cdc_sink(self):
        request = self.command.create_request(self.node)
        self.assertEqual("PUT", request.method)
        self.assertEqual("http://localhost:8080/databases/db1/admin/cdc-sink", request.url)

    def test_configuration_json_is_the_body(self):
        request = self.command.create_request(self.node)
        self.assertEqual("cdc-1", json.loads(request.data)["Name"])

    def test_not_a_read_request(self):
        self.assertFalse(self.command.is_read_request())

    def test_implements_raft_command_marker(self):
        self.assertIsInstance(self.command, RaftCommand)
        self.assertTrue(self.command.get_raft_unique_request_id())

    def test_null_response_raises(self):
        with self.assertRaises(ValueError):
            self.command.set_response(None, False)

    def test_result_parses_raft_command_index_and_task_id(self):
        self.command.set_response(json.dumps({"RaftCommandIndex": 5, "TaskId": 9}), False)
        self.assertEqual(5, self.command.result.raft_command_index)
        self.assertEqual(9, self.command.result.task_id)

    def test_result_round_trips_beyond_2_31(self):
        result = AddCdcSinkOperationResult.from_json({"RaftCommandIndex": 99999999999999, "TaskId": 5000000000})
        self.assertEqual(99999999999999, result.raft_command_index)
        self.assertEqual(5000000000, result.task_id)

    def test_result_missing_keys_default_to_zero(self):
        result = AddCdcSinkOperationResult.from_json({})
        self.assertEqual(0, result.raft_command_index)
        self.assertEqual(0, result.task_id)

    def test_operation_get_command(self):
        operation = AddCdcSinkOperation(self.config)
        self.assertIsInstance(operation.get_command(None), AddCdcSinkCommand)


class TestUpdateCdcSinkOperationWire(unittest.TestCase):
    def setUp(self):
        self.node = ServerNode("http://localhost:8080", "db1")
        self.config = CdcSinkConfiguration(name="cdc-1", connection_string_name="sql-cs")
        self.command = UpdateCdcSinkCommand(None, 42, self.config)

    def test_put_to_admin_cdc_sink_with_id_query(self):
        request = self.command.create_request(self.node)
        self.assertEqual("PUT", request.method)
        self.assertEqual("http://localhost:8080/databases/db1/admin/cdc-sink?id=42", request.url)

    def test_not_a_read_request(self):
        self.assertFalse(self.command.is_read_request())

    def test_implements_raft_command_marker(self):
        self.assertIsInstance(self.command, RaftCommand)
        self.assertTrue(self.command.get_raft_unique_request_id())

    def test_null_response_raises(self):
        with self.assertRaises(ValueError):
            self.command.set_response(None, False)

    def test_result_parses(self):
        self.command.set_response(json.dumps({"RaftCommandIndex": 5, "TaskId": 42}), False)
        self.assertIsInstance(self.command.result, UpdateCdcSinkOperationResult)
        self.assertEqual(5, self.command.result.raft_command_index)
        self.assertEqual(42, self.command.result.task_id)

    def test_result_missing_keys_default_to_zero(self):
        self.command.set_response(json.dumps({}), False)
        self.assertEqual(0, self.command.result.raft_command_index)
        self.assertEqual(0, self.command.result.task_id)

    def test_operation_get_command(self):
        operation = UpdateCdcSinkOperation(42, self.config)
        self.assertIsInstance(operation.get_command(None), UpdateCdcSinkCommand)


class TestExceptionSurfacing(unittest.TestCase):
    def _dispatch(self, error_type, message, error, code=402):
        schema = ExceptionDispatcher.ExceptionSchema(
            url="http://localhost:8080", object_type=error_type, message=message, error=error
        )
        return ExceptionDispatcher.get(schema, code)

    def test_unmapped_type_surfaces_base_raven_exception_with_full_error_text(self):
        exception = self._dispatch(
            "Raven.Client.Exceptions.Commercial.LicenseLimitException",
            "Your license doesn't support using the CDC sink feature.",
            "Raven.Client.Exceptions.Commercial.LicenseLimitException: Your license doesn't support using the CDC sink feature.",
        )
        self.assertIsInstance(exception, RavenException)
        self.assertIn("Your license doesn't support using the CDC sink feature.", str(exception))
        self.assertIn("The server at http://localhost:8080 responded with status code: 402", str(exception))

    def test_500_lookup_failure_message_embeds_inner_error(self):
        error = (
            "System.InvalidOperationException: Invalid CDC Sink configuration.\n"
            "Errors:\n"
            "- Could not find connection string named 'sql-cs'. Please supply an existing connection string.\n"
            "Configuration:\n{json}\n"
        )
        exception = self._dispatch(
            "System.InvalidOperationException",
            "Invalid CDC Sink configuration.",
            error,
            code=500,
        )
        self.assertIsInstance(exception, RavenException)
        self.assertIn(
            "Could not find connection string named 'sql-cs'. Please supply an existing connection string.",
            str(exception),
        )

    def test_500_nonexistent_task_message_embeds_inner_error(self):
        error = (
            "Raven.Server.Rachis.RachisApplyException: Failed to update database record.\n"
            " ---> System.InvalidOperationException: CDC Sink task with ID 999999 does not exist.\n"
        )
        exception = self._dispatch(
            "Raven.Server.Rachis.RachisApplyException",
            "Failed to update database record.",
            error,
            code=500,
        )
        self.assertIsInstance(exception, RavenException)
        self.assertIn("Failed to update database record.", str(exception))
        self.assertIn("CDC Sink task with ID 999999 does not exist.", str(exception))


class TestOngoingTaskType(unittest.TestCase):
    def test_cdc_sink_wire_value(self):
        self.assertEqual("CdcSink", OngoingTaskType.CDC_SINK.value)

    def test_delete_operation_flows_type_to_wire(self):
        operation = DeleteOngoingTaskOperation(42, OngoingTaskType.CDC_SINK)
        command = operation.get_command(None)
        request = command.create_request(ServerNode("http://localhost:8080", "db1"))
        self.assertEqual("http://localhost:8080/databases/db1/admin/tasks?id=42&type=CdcSink", request.url)


class TestOngoingTaskCdcSink(unittest.TestCase):
    def _task_dict(self, **overrides):
        base = {
            "TaskId": 7,
            "TaskType": "CdcSink",
            "ResponsibleNode": {"NodeTag": "A", "NodeUrl": "http://x", "ResponsibleNode": "A"},
            "TaskState": "Enabled",
            "TaskConnectionStatus": "Active",
            "TaskName": "cdc-1",
            "Error": None,
            "MentorNode": "A",
            "PinToMentorNode": True,
            "ConnectionStringName": "sql-cs",
            "FactoryName": "System.Data.SqlClient",
            "Configuration": {
                "Name": "cdc-1",
                "TaskId": 7,
                "Disabled": False,
                "ConnectionStringName": "sql-cs",
                "MentorNode": None,
                "PinToMentorNode": True,
                "Tables": [],
                "Postgres": None,
                "SkipInitialLoad": False,
            },
            "LastBatchTime": "2026-08-18T10:00:00.0000000Z",
            "LastCheckpoint": "0/1",
            "SecondsSinceLastBatch": 12.5,
            "LastActivityTime": "2026-08-18T10:05:00.0000000Z",
            "SecondsSinceLastActivity": 3.25,
            "HealthIssue": None,
        }
        base.update(overrides)
        return base

    def test_from_json_parses_all_keys(self):
        task = OngoingTaskCdcSink.from_json(self._task_dict())
        self.assertIsInstance(task, OngoingTaskCdcSink)
        self.assertEqual(OngoingTaskType.CDC_SINK, task.task_type)
        self.assertEqual(7, task.task_id)
        self.assertEqual("cdc-1", task.task_name)
        self.assertEqual("sql-cs", task.connection_string_name)
        self.assertEqual("System.Data.SqlClient", task.factory_name)
        self.assertEqual("0/1", task.last_checkpoint)
        self.assertEqual(12.5, task.seconds_since_last_batch)
        self.assertEqual(3.25, task.seconds_since_last_activity)
        self.assertIsNone(task.health_issue)

    def test_datetimes_parse_as_naive_utc(self):
        task = OngoingTaskCdcSink.from_json(self._task_dict())
        self.assertEqual("2026-08-18 10:00:00", str(task.last_batch_time))
        self.assertEqual("2026-08-18 10:05:00", str(task.last_activity_time))
        self.assertIsNone(task.last_batch_time.tzinfo)

    def test_configuration_parses_as_object(self):
        task = OngoingTaskCdcSink.from_json(self._task_dict())
        from ravendb.documents.operations.cdc_sink.cdc_sink_configuration import CdcSinkConfiguration

        self.assertIsInstance(task.configuration, CdcSinkConfiguration)
        self.assertEqual("cdc-1", task.configuration.name)

    def test_nullable_fields_stay_none_when_absent(self):
        task = OngoingTaskCdcSink.from_json(self._task_dict())
        task = OngoingTaskCdcSink.from_json(
            {
                "TaskId": 7,
                "TaskType": "CdcSink",
                "TaskName": "cdc-1",
            }
        )
        self.assertIsNone(task.last_batch_time)
        self.assertIsNone(task.last_checkpoint)
        self.assertIsNone(task.seconds_since_last_batch)
        self.assertIsNone(task.configuration)

    def test_to_json_key_order(self):
        task = OngoingTaskCdcSink.from_json(self._task_dict())
        out = task.to_json()
        self.assertEqual(
            [
                "TaskId",
                "TaskType",
                "ResponsibleNode",
                "TaskState",
                "TaskConnectionStatus",
                "TaskName",
                "Error",
                "MentorNode",
                "PinToMentorNode",
                "ConnectionStringName",
                "FactoryName",
                "Configuration",
                "LastBatchTime",
                "LastCheckpoint",
                "SecondsSinceLastBatch",
                "LastActivityTime",
                "SecondsSinceLastActivity",
                "HealthIssue",
            ],
            list(out.keys()),
        )
        self.assertEqual("CdcSink", out["TaskType"])
        self.assertEqual("System.Data.SqlClient", out["FactoryName"])

    def test_configuration_serializes_with_type_omission_rule(self):
        task = OngoingTaskCdcSink.from_json(self._task_dict())
        configuration_out = task.to_json()["Configuration"]
        self.assertNotIn("TestMode", configuration_out)
        self.assertIsNone(configuration_out["Postgres"])

    def test_to_json_writes_none_for_unset_configuration(self):
        task = OngoingTaskCdcSink.from_json({"TaskId": 1, "TaskType": "CdcSink"})
        self.assertIsNone(task.to_json()["Configuration"])

    def test_fractional_seconds_round_trip(self):
        task = OngoingTaskCdcSink.from_json(self._task_dict())
        out = task.to_json()
        self.assertEqual(12.5, out["SecondsSinceLastBatch"])
        self.assertEqual(3.25, out["SecondsSinceLastActivity"])


class TestGetOngoingTaskInfoDispatch(unittest.TestCase):
    def test_cdc_sink_dispatches_to_ongoing_task_cdc_sink(self):
        operation = GetOngoingTaskInfoOperation(7, OngoingTaskType.CDC_SINK)
        command = operation.get_command(None)
        json_dict = {
            "TaskId": 7,
            "TaskType": "CdcSink",
            "ConnectionStringName": "sql-cs",
            "Configuration": None,
        }
        task = command._deserialize_task(json_dict)
        self.assertIsInstance(task, OngoingTaskCdcSink)

    def test_get_request_url(self):
        operation = GetOngoingTaskInfoOperation(7, OngoingTaskType.CDC_SINK)
        command = operation.get_command(None)
        request = command.create_request(ServerNode("http://localhost:8080", "db1"))
        self.assertEqual("http://localhost:8080/databases/db1/task?key=7&type=CdcSink", request.url)


class TestDatabaseRecordCdcSinks(unittest.TestCase):
    def _base_record(self, **overrides):
        record = {"DatabaseName": "db", "LockMode": "Unlock", "AutoIndexes": {}}
        record.update(overrides)
        return record

    def test_from_json_parses_cdc_sinks(self):
        raw = self._base_record(
            CdcSinks=[
                {
                    "Name": "cdc-1",
                    "TaskId": 9,
                    "Disabled": False,
                    "ConnectionStringName": "sql-cs",
                    "MentorNode": None,
                    "PinToMentorNode": False,
                    "Tables": [],
                    "Postgres": None,
                    "SkipInitialLoad": False,
                }
            ]
        )
        record = DatabaseRecord.from_json(raw)
        self.assertEqual(1, len(record.cdc_sinks))
        self.assertEqual("cdc-1", record.cdc_sinks[0].name)
        self.assertEqual("sql-cs", record.cdc_sinks[0].connection_string_name)

    def test_missing_cdc_sinks_key_yields_empty_list(self):
        record = DatabaseRecord.from_json(self._base_record())
        self.assertEqual([], record.cdc_sinks)

    def test_to_json_emits_cdc_sinks_with_configuration_shape(self):
        record = DatabaseRecord.from_json(
            self._base_record(
                CdcSinks=[
                    {
                        "Name": "cdc-1",
                        "TaskId": 9,
                        "Disabled": False,
                        "ConnectionStringName": "sql-cs",
                        "MentorNode": None,
                        "PinToMentorNode": False,
                        "Tables": [],
                        "Postgres": None,
                        "SkipInitialLoad": False,
                    }
                ]
            )
        )
        out = record.to_json()
        self.assertIn("CdcSinks", out)
        self.assertEqual("cdc-1", out["CdcSinks"][0]["Name"])

    def test_empty_cdc_sinks_serialize_as_empty_array(self):
        record = DatabaseRecord.from_json(self._base_record())
        self.assertEqual([], record.to_json()["CdcSinks"])


if __name__ == "__main__":
    unittest.main()
