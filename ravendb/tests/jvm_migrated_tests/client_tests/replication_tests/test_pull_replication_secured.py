import base64
import hashlib
import os
import re
import unittest

from ravendb import (
    GetPullReplicationTasksInfoOperation,
    GetReplicationHubAccessOperation,
    PreventDeletionsMode,
    PullReplicationAsSink,
    PullReplicationDefinition,
    PullReplicationMode,
    PutPullReplicationAsHubOperation,
    RegisterReplicationHubAccessOperation,
    ReplicationHubAccess,
    UnregisterReplicationHubAccessOperation,
    UpdatePullReplicationAsSinkOperation,
)
from ravendb.documents.operations.connection_string.put_connection_string_operation import (
    PutConnectionStringOperation,
)
from ravendb.documents.operations.etl.configuration import RavenConnectionString
from ravendb.documents.operations.ongoing_tasks import (
    GetOngoingTaskInfoOperation,
    OngoingTaskPullReplicationAsSink,
    OngoingTaskType,
)
from ravendb.exceptions.raven_exceptions import RavenException
from ravendb.tests.test_base import TestBase


def _certificate_base64_and_thumbprint(pem_path):
    """Extract the certificate (base64 DER) and its SHA-1 thumbprint from a PEM file."""
    with open(pem_path, "r") as pem_file:
        pem = pem_file.read()
    match = re.search(r"-----BEGIN CERTIFICATE-----(.+?)-----END CERTIFICATE-----", pem, re.DOTALL)
    certificate_base64 = "".join(match.group(1).split())
    der = base64.b64decode(certificate_base64)
    return certificate_base64, hashlib.sha1(der).hexdigest().upper()


@unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
class TestPullReplicationSecured(TestBase):
    """Pull-replication surface that the server only permits over SSL (filtering, the
    SinkToHub direction, and certificate-based hub access) - exercised against a secured store."""

    def setUp(self):
        super().setUp()

    def test_can_define_hub_with_filtering_and_combined_mode(self):
        store = self.secured_document_store
        definition = PullReplicationDefinition(
            "filtered-hub",
            mode=PullReplicationMode.HUB_TO_SINK_AND_SINK_TO_HUB,
            with_filtering=True,
            prevent_deletions_mode=PreventDeletionsMode.PREVENT_SINK_TO_HUB_DELETIONS,
        )

        result = store.maintenance.send(PutPullReplicationAsHubOperation(definition))
        self.assertIsNotNone(result.task_id)

        info = store.maintenance.send(GetPullReplicationTasksInfoOperation(result.task_id))
        self.assertEqual("filtered-hub", info.definition.name)
        self.assertEqual(PullReplicationMode.HUB_TO_SINK_AND_SINK_TO_HUB, info.definition.mode)
        self.assertTrue(info.definition.with_filtering)
        self.assertEqual(PreventDeletionsMode.PREVENT_SINK_TO_HUB_DELETIONS, info.definition.prevent_deletions_mode)

    def test_register_get_and_unregister_replication_hub_access(self):
        store = self.secured_document_store
        store.maintenance.send(
            PutPullReplicationAsHubOperation(PullReplicationDefinition("access-hub", with_filtering=True))
        )

        certificate_base64, thumbprint = _certificate_base64_and_thumbprint(self.test_client_certificate_url)
        access = ReplicationHubAccess(
            name="sink-access", certificate_base64=certificate_base64, allowed_hub_to_sink_paths=["users/*"]
        )
        store.maintenance.send(RegisterReplicationHubAccessOperation("access-hub", access))

        registered = store.maintenance.send(GetReplicationHubAccessOperation("access-hub"))
        self.assertEqual(1, len(registered))
        self.assertEqual(thumbprint.lower(), registered[0].thumbprint.lower())
        self.assertEqual(1, len(registered[0].allowed_hub_to_sink_paths))

        store.maintenance.send(UnregisterReplicationHubAccessOperation("access-hub", thumbprint))
        self.assertEqual([], store.maintenance.send(GetReplicationHubAccessOperation("access-hub")))

    def test_register_replication_hub_access_for_missing_hub_raises(self):
        store = self.secured_document_store
        certificate_base64, _ = _certificate_base64_and_thumbprint(self.test_client_certificate_url)
        operation = RegisterReplicationHubAccessOperation(
            "hub-that-does-not-exist", ReplicationHubAccess(name="sink", certificate_base64=certificate_base64)
        )
        # The client maps a 404 to ReplicationHubNotFoundException; the 7.2 server reports a
        # missing hub as a 500 instead, so assert the base RavenException (covers either).
        self.assertRaises(RavenException, store.maintenance.send, operation)

    def test_can_define_sink_with_server_certificate(self):
        store = self.secured_document_store
        connection_string = RavenConnectionString(
            name="cs-secured-sink", database="sink-remote-db", topology_discovery_urls=list(store.urls)
        )
        store.maintenance.send(PutConnectionStringOperation(connection_string))

        sink = PullReplicationAsSink(
            database=store.database,
            connection_string_name="cs-secured-sink",
            hub_name="remote-hub",
            name="server-cert-sink",
        )
        result = store.maintenance.send(UpdatePullReplicationAsSinkOperation(sink, use_server_certificate=True))
        self.assertIsNotNone(result.task_id)

        info = store.maintenance.send(
            GetOngoingTaskInfoOperation(result.task_id, OngoingTaskType.PULL_REPLICATION_AS_SINK)
        )
        self.assertIsInstance(info, OngoingTaskPullReplicationAsSink)
        # use_server_certificate sends no client key; the secured server still accepts and
        # stores the sink (it authenticates with its own certificate at connection time).
        self.assertEqual("remote-hub", info.hub_name)
        self.assertEqual("server-cert-sink", info.task_name)
        self.assertEqual("cs-secured-sink", info.connection_string_name)
