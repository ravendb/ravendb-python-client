import http
import os
import unittest
from datetime import timedelta
from types import SimpleNamespace

from ravendb import (
    ExternalReplication,
    GetReplicationHubAccessOperation,
    GetPullReplicationTasksInfoOperation,
    PreventDeletionsMode,
    PullReplicationAsSink,
    PullReplicationDefinition,
    PullReplicationMode,
    PutPullReplicationAsHubOperation,
    RegisterReplicationHubAccessOperation,
    ReplicationHubAccess,
    UnregisterReplicationHubAccessOperation,
    UpdateExternalReplicationOperation,
    UpdatePullReplicationAsSinkOperation,
)
from ravendb.documents.operations.etl.configuration import RavenConnectionString
from ravendb.documents.operations.connection_string.put_connection_string_operation import (
    PutConnectionStringOperation,
)
from ravendb.documents.operations.ongoing_tasks import (
    GetOngoingTaskInfoOperation,
    OngoingTaskPullReplicationAsSink,
    OngoingTaskType,
)
from ravendb.exceptions.raven_exceptions import ReplicationHubNotFoundException
from ravendb.tests.test_base import TestBase


class TestPullReplication(TestBase):
    def setUp(self):
        super().setUp()

    def _put_hub(self, name, **kwargs):
        definition = PullReplicationDefinition(name, **kwargs)
        result = self.store.maintenance.send(PutPullReplicationAsHubOperation(definition))
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.task_id)
        return result

    def _put_connection_string(self, name, database):
        connection_string = RavenConnectionString(
            name=name, database=database, topology_discovery_urls=list(self.store.urls)
        )
        self.store.maintenance.send(PutConnectionStringOperation(connection_string))
        return name

    @unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
    def test_can_put_pull_replication_as_hub(self):
        result = self._put_hub("hub-1")
        self.assertGreater(result.task_id, 0)

    @unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
    def test_can_put_pull_replication_as_hub_by_name(self):
        result = self.store.maintenance.send(PutPullReplicationAsHubOperation(name="hub-by-name"))
        self.assertIsNotNone(result.task_id)

    @unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
    def test_can_get_pull_replication_tasks_info(self):
        result = self._put_hub("hub-info", delay_replication_for=timedelta(seconds=30), disabled=True)

        info = self.store.maintenance.send(GetPullReplicationTasksInfoOperation(result.task_id))
        self.assertIsNotNone(info)
        self.assertIsNotNone(info.definition)
        self.assertEqual("hub-info", info.definition.name)
        self.assertEqual(PullReplicationMode.HUB_TO_SINK, info.definition.mode)
        self.assertEqual(timedelta(seconds=30), info.definition.delay_replication_for)
        self.assertTrue(info.definition.disabled)
        self.assertIsNotNone(info.ongoing_tasks)

    @unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
    def test_can_update_pull_replication_as_sink(self):
        connection_string_name = self._put_connection_string("cs-sink", "sink-remote-db")
        sink = PullReplicationAsSink(
            database=self.store.database,
            connection_string_name=connection_string_name,
            hub_name="remote-hub",
            name="my-sink",
        )

        result = self.store.maintenance.send(UpdatePullReplicationAsSinkOperation(sink))
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.task_id)

        info = self.store.maintenance.send(
            GetOngoingTaskInfoOperation(result.task_id, OngoingTaskType.PULL_REPLICATION_AS_SINK)
        )
        self.assertIsInstance(info, OngoingTaskPullReplicationAsSink)
        self.assertEqual("remote-hub", info.hub_name)
        self.assertEqual("my-sink", info.task_name)
        self.assertEqual(PullReplicationMode.HUB_TO_SINK, info.mode)
        self.assertEqual(connection_string_name, info.connection_string_name)
        self.assertEqual("sink-remote-db", info.destination_database)

    @unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
    def test_can_update_external_replication(self):
        connection_string_name = self._put_connection_string("cs-ext", "external-remote-db")
        external = ExternalReplication(
            database=self.store.database,
            connection_string_name=connection_string_name,
            name="my-external-replication",
        )

        result = self.store.maintenance.send(UpdateExternalReplicationOperation(external))
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.task_id)

    @unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
    def test_get_replication_hub_access_is_empty_for_new_hub(self):
        self._put_hub("hub-no-access")
        access = self.store.maintenance.send(GetReplicationHubAccessOperation("hub-no-access"))
        self.assertEqual([], access)

    def test_register_hub_access_maps_404_to_replication_hub_not_found(self):
        # The register command reads a raw response; a 404 (hub not found) must surface as
        # ReplicationHubNotFoundException, matching the C# client.
        command = RegisterReplicationHubAccessOperation("missing-hub", ReplicationHubAccess(name="s")).get_command(None)
        self.assertRaises(
            ReplicationHubNotFoundException,
            command.set_response_raw,
            SimpleNamespace(status_code=http.HTTPStatus.NOT_FOUND),
            None,
        )
        # A successful (non-404) response must not raise.
        command.set_response_raw(SimpleNamespace(status_code=http.HTTPStatus.OK), None)

    @unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
    def test_unregister_replication_hub_access_is_noop_for_unknown_thumbprint(self):
        self._put_hub("hub-unregister")
        # Removing an unknown thumbprint from an existing hub must not raise.
        self.store.maintenance.send(
            UnregisterReplicationHubAccessOperation("hub-unregister", "0123456789ABCDEF0123456789ABCDEF01234567")
        )

    def test_operations_validate_arguments_client_side(self):
        self.assertRaises(ValueError, lambda: PutPullReplicationAsHubOperation())
        self.assertRaises(ValueError, lambda: RegisterReplicationHubAccessOperation("", ReplicationHubAccess()))
        self.assertRaises(ValueError, lambda: RegisterReplicationHubAccessOperation("hub", None))
        self.assertRaises(ValueError, lambda: UnregisterReplicationHubAccessOperation("hub", ""))
        self.assertRaises(ValueError, lambda: GetReplicationHubAccessOperation(""))
        self.assertRaises(ValueError, lambda: UpdatePullReplicationAsSinkOperation(None))

    def test_pull_replication_definition_json_round_trip(self):
        definition = PullReplicationDefinition(
            "round-trip",
            delay_replication_for=timedelta(minutes=5),
            mode=PullReplicationMode.HUB_TO_SINK_AND_SINK_TO_HUB,
            with_filtering=True,
            prevent_deletions_mode=PreventDeletionsMode.PREVENT_SINK_TO_HUB_DELETIONS,
            disabled=True,
        )

        parsed = PullReplicationDefinition.from_json(definition.to_json())
        self.assertEqual("round-trip", parsed.name)
        self.assertEqual(PullReplicationMode.HUB_TO_SINK_AND_SINK_TO_HUB, parsed.mode)
        self.assertTrue(parsed.with_filtering)
        self.assertTrue(parsed.disabled)
        self.assertEqual(PreventDeletionsMode.PREVENT_SINK_TO_HUB_DELETIONS, parsed.prevent_deletions_mode)
        self.assertEqual(timedelta(minutes=5), parsed.delay_replication_for)

    def test_pull_replication_as_sink_json_round_trip(self):
        sink = PullReplicationAsSink(
            database="db",
            connection_string_name="cs",
            hub_name="h",
            name="s",
            mode=PullReplicationMode.HUB_TO_SINK,
            allowed_hub_to_sink_paths=["users/*"],
        )

        parsed = PullReplicationAsSink.from_json(sink.to_json())
        self.assertEqual("h", parsed.hub_name)
        self.assertEqual("cs", parsed.connection_string_name)
        self.assertEqual("db", parsed.database)
        self.assertEqual("s", parsed.name)
        self.assertEqual(PullReplicationMode.HUB_TO_SINK, parsed.mode)
        self.assertEqual(["users/*"], parsed.allowed_hub_to_sink_paths)

    def test_replication_node_to_json_includes_type(self):
        sink_json = PullReplicationAsSink(database="db", connection_string_name="cs", hub_name="h").to_json()
        self.assertEqual("PullAsSink", sink_json["Type"])
        external_json = ExternalReplication(database="db", connection_string_name="cs").to_json()
        self.assertEqual("External", external_json["Type"])

    def test_update_sink_with_server_certificate_omits_private_key(self):
        node = SimpleNamespace(url="http://localhost:8080", database="db")
        sink = PullReplicationAsSink(
            database="db", connection_string_name="cs", hub_name="h", name="s", certificate_with_private_key="SECRET"
        )

        with_server_cert = UpdatePullReplicationAsSinkOperation(sink, use_server_certificate=True).get_command(None)
        sent = with_server_cert.create_request(node).data["PullReplicationAsSink"]
        self.assertIsNone(sent["CertificateWithPrivateKey"])

        without = UpdatePullReplicationAsSinkOperation(sink, use_server_certificate=False).get_command(None)
        sent = without.create_request(node).data["PullReplicationAsSink"]
        self.assertEqual("SECRET", sent["CertificateWithPrivateKey"])
