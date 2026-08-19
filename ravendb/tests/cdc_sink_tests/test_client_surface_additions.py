"""Tests for the client version header, the S3 checksum-validation flag, the
connection-string UsedBy metadata, and the pull-replication sink cursors.
"""

import json
import unittest

import requests

from ravendb.documents.operations.attachments import RemoteAttachmentsS3Settings
from ravendb.documents.operations.backups.settings import (
    GetBackupConfigurationScript,
    S3Settings,
)
from ravendb.documents.operations.connection_string.get_connection_string_operation import (
    GetConnectionStringsOperation,
    GetConnectionStringsResult,
)
from ravendb.documents.operations.connection_strings import ConnectionStringUsage
from ravendb.documents.operations.ongoing_tasks import (
    OngoingTaskPullReplicationAsSink,
    OngoingTaskType,
)
from ravendb.http.request_executor import RequestExecutor
from ravendb.http.server_node import ServerNode
from ravendb.primitives import constants


class TestClientVersionHeader(unittest.TestCase):
    def test_client_version_is_7_2_5(self):
        self.assertEqual("7.2.5", RequestExecutor.CLIENT_VERSION)

    def test_wire_header_carries_client_version(self):
        executor = object.__new__(RequestExecutor)
        executor._disable_client_configuration_updates = False
        executor._client_configuration_etag = 0
        executor._disable_topology_updates = False
        executor._topology_etag = 0

        request = requests.Request("GET", "http://localhost:8080/databases/db1/")
        executor._set_request_headers(None, None, request)
        self.assertEqual("7.2.5", request.headers[constants.Headers.CLIENT_VERSION])


class TestS3ChecksumValidation(unittest.TestCase):
    def _script(self):
        return GetBackupConfigurationScript.default()

    def test_to_json_writes_disable_checksum_validation(self):
        settings = S3Settings(
            bucket_name="b", disable_checksum_validation=True, get_backup_configuration_script=self._script()
        )
        out = settings.to_json()
        self.assertTrue(out["DisableChecksumValidation"])

    def test_from_json_parses_flag(self):
        script = self._script().to_json()
        settings = S3Settings.from_json(
            {
                "Disabled": False,
                "GetBackupConfigurationScript": script,
                "AwsAccessKey": None,
                "AwsSecretKey": None,
                "AwsSessionToken": None,
                "AwsRegionName": None,
                "RemoteFolderName": None,
                "BucketName": "b",
                "CustomServerUrl": None,
                "ForcePathStyle": False,
                "DisableChecksumValidation": True,
            }
        )
        self.assertTrue(settings.disable_checksum_validation)

    def test_from_json_defaults_flag_to_false_when_absent(self):
        script = self._script().to_json()
        settings = S3Settings.from_json(
            {
                "Disabled": False,
                "GetBackupConfigurationScript": script,
                "AwsAccessKey": None,
                "AwsSecretKey": None,
                "AwsSessionToken": None,
                "AwsRegionName": None,
                "RemoteFolderName": None,
                "BucketName": "b",
                "CustomServerUrl": None,
                "ForcePathStyle": False,
            }
        )
        self.assertFalse(settings.disable_checksum_validation)

    def test_equality_includes_flag(self):
        a = S3Settings(bucket_name="b", disable_checksum_validation=False)
        b = S3Settings(bucket_name="b", disable_checksum_validation=True)
        self.assertNotEqual(a, b)
        same = S3Settings(bucket_name="b", disable_checksum_validation=False)
        self.assertEqual(a, same)

    def test_stays_hashable_and_hash_consistent_with_equality(self):
        a = S3Settings(bucket_name="b", disable_checksum_validation=False)
        same = S3Settings(bucket_name="b", disable_checksum_validation=False)
        b = S3Settings(bucket_name="b", disable_checksum_validation=True)
        self.assertEqual(hash(a), hash(same))
        self.assertNotEqual(hash(a), hash(b))
        self.assertEqual(len({a, same, b}), 2)


class TestRemoteAttachmentsS3ChecksumValidation(unittest.TestCase):
    def test_to_json_writes_disable_checksum_validation(self):
        settings = RemoteAttachmentsS3Settings(bucket_name="b", disable_checksum_validation=True)
        self.assertTrue(settings.to_json()["DisableChecksumValidation"])

    def test_from_json_parses_flag_and_defaults_false(self):
        self.assertTrue(
            RemoteAttachmentsS3Settings.from_json(
                {"BucketName": "b", "DisableChecksumValidation": True}
            ).disable_checksum_validation
        )
        self.assertFalse(RemoteAttachmentsS3Settings.from_json({"BucketName": "b"}).disable_checksum_validation)

    def test_equality_includes_flag(self):
        a = RemoteAttachmentsS3Settings(bucket_name="b", disable_checksum_validation=False)
        b = RemoteAttachmentsS3Settings(bucket_name="b", disable_checksum_validation=True)
        self.assertNotEqual(a, b)
        self.assertEqual(a, RemoteAttachmentsS3Settings(bucket_name="b", disable_checksum_validation=False))

    def test_stays_hashable_and_hash_consistent_with_equality(self):
        a = RemoteAttachmentsS3Settings(bucket_name="b", disable_checksum_validation=False)
        same = RemoteAttachmentsS3Settings(bucket_name="b", disable_checksum_validation=False)
        b = RemoteAttachmentsS3Settings(bucket_name="b", disable_checksum_validation=True)
        self.assertEqual(hash(a), hash(same))
        self.assertNotEqual(hash(a), hash(b))
        self.assertEqual(len({a, same, b}), 2)


class TestConnectionStringUsedBy(unittest.TestCase):
    def _result_dict(self, used_by=None):
        return {
            "RavenConnectionStrings": {
                "raven": {
                    "Name": "raven",
                    "Database": "db",
                    "TopologyDiscoveryUrls": [],
                    "UsedBy": used_by if used_by is not None else [],
                }
            },
            "SqlConnectionStrings": {},
            "OlapConnectionStrings": {},
            "AiConnectionStrings": {},
            "ElasticSearchConnectionStrings": {},
            "QueueConnectionStrings": {},
            "SnowflakeConnectionStrings": {},
        }

    def test_used_by_parsed_from_get_response(self):
        result = GetConnectionStringsResult.from_json(
            self._result_dict(
                used_by=[
                    {
                        "Kind": "SqlEtl",
                        "Id": 5000000000,
                        "Identifier": None,
                        "Name": "etl-1",
                    },
                    {"Kind": "AiAgent", "Id": None, "Identifier": "agent-1", "Name": "agent-1"},
                ]
            )
        )
        connection_string = result.raven_connection_strings["raven"]
        self.assertEqual(2, len(connection_string.used_by))
        first = connection_string.used_by[0]
        self.assertIsInstance(first, ConnectionStringUsage)
        self.assertEqual("SqlEtl", first.kind)
        self.assertEqual(5000000000, first.id)
        self.assertIsNone(first.identifier)
        self.assertEqual("etl-1", first.name)
        second = connection_string.used_by[1]
        self.assertEqual("AiAgent", second.kind)
        self.assertIsNone(second.id)
        self.assertEqual("agent-1", second.identifier)

    def test_used_by_empty_array_when_nothing_uses_it(self):
        result = GetConnectionStringsResult.from_json(self._result_dict())
        self.assertEqual([], result.raven_connection_strings["raven"].used_by)

    def test_absent_used_by_key_does_not_raise(self):
        raw = self._result_dict()
        del raw["RavenConnectionStrings"]["raven"]["UsedBy"]
        result = GetConnectionStringsResult.from_json(raw)
        self.assertEqual([], result.raven_connection_strings["raven"].used_by)

    def test_to_json_never_emits_used_by(self):
        result = GetConnectionStringsResult.from_json(self._result_dict(used_by=[{"Kind": "SqlEtl", "Id": 1}]))
        connection_string = result.raven_connection_strings["raven"]
        out = connection_string.to_json()
        self.assertNotIn("UsedBy", out)
        # round-trip: GET -> from_json -> to_json -> PUT must not leak UsedBy
        self.assertNotIn("UsedBy", json.dumps(out, default=str))

    def test_get_command_is_a_read_request(self):
        operation = GetConnectionStringsOperation()
        command = operation.get_command(None)
        self.assertTrue(command.is_read_request())
        request = command.create_request(ServerNode("http://localhost:8080", "db1"))
        self.assertEqual("http://localhost:8080/databases/db1/admin/connection-strings", request.url)


class TestPullReplicationAsSinkCursors(unittest.TestCase):
    def _sink_dict(self, **overrides):
        base = {
            "TaskId": 1,
            "TaskType": "PullReplicationAsSink",
            "HubName": "hub",
            "AllowedHubToSinkPaths": ["a"],
            "AllowedSinkToHubPaths": ["b"],
            "HubCursor": "hub-cursor-1",
            "SinkCursor": "sink-cursor-1",
        }
        base.update(overrides)
        return base

    def test_from_json_parses_hub_and_sink_cursor(self):
        task = OngoingTaskPullReplicationAsSink.from_json(self._sink_dict())
        self.assertEqual("hub-cursor-1", task.hub_cursor)
        self.assertEqual("sink-cursor-1", task.sink_cursor)

    def test_to_json_writes_cursors_after_allowed_sink_to_hub_paths(self):
        task = OngoingTaskPullReplicationAsSink.from_json(self._sink_dict())
        out = task.to_json()
        self.assertEqual("hub-cursor-1", out["HubCursor"])
        self.assertEqual("sink-cursor-1", out["SinkCursor"])
        keys = list(out.keys())
        self.assertGreater(keys.index("HubCursor"), keys.index("AllowedSinkToHubPaths"))
        self.assertEqual(["HubCursor", "SinkCursor"], keys[-2:])

    def test_absent_keys_stay_none_and_serialize_as_none(self):
        task = OngoingTaskPullReplicationAsSink.from_json(self._sink_dict(HubCursor=None))
        self.assertIsNone(task.hub_cursor)
        task = OngoingTaskPullReplicationAsSink.from_json({"TaskId": 1, "TaskType": "PullReplicationAsSink"})
        self.assertIsNone(task.hub_cursor)
        self.assertIsNone(task.sink_cursor)
        out = task.to_json()
        self.assertIsNone(out["HubCursor"])
        self.assertIsNone(out["SinkCursor"])

    def test_round_trip(self):
        task = OngoingTaskPullReplicationAsSink.from_json(self._sink_dict())
        parsed = OngoingTaskPullReplicationAsSink.from_json(task.to_json())
        self.assertEqual("hub-cursor-1", parsed.hub_cursor)
        self.assertEqual("sink-cursor-1", parsed.sink_cursor)


if __name__ == "__main__":
    unittest.main()
