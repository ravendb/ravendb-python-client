"""Integration tests for the CDC Sink task surface against a live RavenDB server.

The server gates CDC Sink behind a license, so the tests follow the AI-agent
integration pattern: skipped when RAVENDB_LICENSE is not set. The add request
is attempted through the standard maintenance executor; a server rejection
surfaces as a base RavenException whose message embeds the server error text,
and an accepted task is read back through GetOngoingTaskInfoOperation to
verify the OngoingTaskType.CDC_SINK dispatch.
"""

import os
import unittest

from ravendb.exceptions.raven_exceptions import RavenException
from ravendb.documents.operations.cdc_sink import (
    AddCdcSinkOperation,
    CdcColumnMapping,
    CdcSinkConfiguration,
    CdcSinkTableConfig,
    UpdateCdcSinkOperation,
)
from ravendb.documents.operations.connection_string.put_connection_string_operation import (
    PutConnectionStringOperation,
)
from ravendb.documents.operations.connection_string.remove_connection_string_operation import (
    RemoveConnectionStringOperation,
)
from ravendb.documents.operations.etl.sql import SqlConnectionString
from ravendb.documents.operations.ongoing_tasks import (
    DeleteOngoingTaskOperation,
    GetOngoingTaskInfoOperation,
    OngoingTaskCdcSink,
    OngoingTaskType,
)
from ravendb.tests.test_base import TestBase


@unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
class TestCdcSinkIntegration(TestBase):
    CONNECTION_STRING_NAME = "cdc-sink-test-sql-cs"

    def setUp(self):
        super().setUp()
        sql_connection_string = SqlConnectionString(
            name=self.CONNECTION_STRING_NAME,
            connection_string="Data Source=localhost;Initial Catalog=test;Integrated Security=true",
            factory_name="System.Data.SqlClient",
        )
        self.store.maintenance.send(PutConnectionStringOperation(sql_connection_string))
        self._created_task_ids = []

    def tearDown(self):
        for task_id in self._created_task_ids:
            try:
                self.store.maintenance.send(DeleteOngoingTaskOperation(task_id, OngoingTaskType.CDC_SINK))
            except Exception:
                pass
        try:
            self.store.maintenance.send(
                RemoveConnectionStringOperation(
                    SqlConnectionString(name=self.CONNECTION_STRING_NAME, connection_string="", factory_name="")
                )
            )
        except Exception:
            pass
        super().tearDown()

    def _valid_config(self, name="cdc-sink-integration-task"):
        return CdcSinkConfiguration(
            name=name,
            connection_string_name=self.CONNECTION_STRING_NAME,
            tables=[
                CdcSinkTableConfig(
                    collection_name="Orders",
                    source_table_schema="dbo",
                    source_table_name="orders",
                    columns=[
                        CdcColumnMapping(column="id", name="Id"),
                        CdcColumnMapping(column="customer", name="Customer"),
                    ],
                    primary_key_columns=["id"],
                )
            ],
        )

    def test_add_read_back_and_delete_cdc_sink_task(self):
        config = self._valid_config()
        try:
            add_result = self.store.maintenance.send(AddCdcSinkOperation(config))
        except RavenException as e:
            # Server rejected the task (e.g. community server without a CDC Sink
            # license): the rejection must surface as a RavenException whose message
            # embeds the server's error text, never the Message field alone.
            self.assertIsInstance(e, RavenException)
            self.assertIn("Your license doesn't support using the CDC sink feature.", str(e))
            return

        self.assertIsNotNone(add_result.raft_command_index)
        self.assertIsNotNone(add_result.task_id)
        self._created_task_ids.append(add_result.task_id)

        ongoing_task = self.store.maintenance.send(
            GetOngoingTaskInfoOperation(add_result.task_id, OngoingTaskType.CDC_SINK)
        )
        self.assertIsInstance(ongoing_task, OngoingTaskCdcSink)
        self.assertEqual(OngoingTaskType.CDC_SINK, ongoing_task.task_type)
        self.assertEqual(self.CONNECTION_STRING_NAME, ongoing_task.connection_string_name)
        self.assertEqual("cdc-sink-integration-task", ongoing_task.configuration.name)

    def test_update_cdc_sink_task(self):
        config = self._valid_config()
        try:
            add_result = self.store.maintenance.send(AddCdcSinkOperation(config))
        except RavenException as e:
            self.assertIsInstance(e, RavenException)
            self.assertIn("Your license doesn't support using the CDC sink feature.", str(e))
            return

        self._created_task_ids.append(add_result.task_id)

        config.disabled = True
        config.task_id = add_result.task_id
        update_result = self.store.maintenance.send(UpdateCdcSinkOperation(add_result.task_id, config))
        self.assertIsNotNone(update_result.raft_command_index)
        # The update deletes and re-adds the task; the response TaskId is the
        # raft index of the update command (the new task id), not the old one.
        self.assertNotEqual(add_result.task_id, update_result.task_id)
        self._created_task_ids.append(update_result.task_id)

        ongoing_task = self.store.maintenance.send(
            GetOngoingTaskInfoOperation(update_result.task_id, OngoingTaskType.CDC_SINK)
        )
        self.assertIsInstance(ongoing_task, OngoingTaskCdcSink)
        self.assertEqual(update_result.task_id, ongoing_task.task_id)
        self.assertTrue(ongoing_task.configuration.disabled)


if __name__ == "__main__":
    unittest.main()
