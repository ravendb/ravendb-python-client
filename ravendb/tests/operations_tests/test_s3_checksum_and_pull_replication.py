"""S3 checksum toggle and pull-replication cursor wire tests."""

import unittest

from ravendb.documents.operations.attachments import RemoteAttachmentsS3Settings
from ravendb.documents.operations.backups.settings import S3Settings
from ravendb.documents.operations.ongoing_tasks import (
    OngoingTaskPullReplicationAsSink,
    OngoingTaskType,
)


class TestS3ChecksumToggleWireShape(unittest.TestCase):
    def test_remote_attachments_s3_settings_to_json_writes_checksum_flag(self):
        settings = RemoteAttachmentsS3Settings(
            aws_access_key="ak",
            aws_secret_key="sk",
            bucket_name="bucket",
            disable_checksum_validation=True,
        )
        payload = settings.to_json()
        self.assertTrue(payload["DisableChecksumValidation"])
        self.assertIn("DisableChecksumValidation", payload)

    def test_remote_attachments_s3_settings_from_json_round_trip(self):
        parsed = RemoteAttachmentsS3Settings.from_json(
            {
                "AwsAccessKey": "ak",
                "AwsSecretKey": "sk",
                "BucketName": "bucket",
                "DisableChecksumValidation": True,
            }
        )
        self.assertTrue(parsed.disable_checksum_validation)
        self.assertEqual("bucket", parsed.bucket_name)

    def test_remote_attachments_s3_settings_default_is_false(self):
        settings = RemoteAttachmentsS3Settings(bucket_name="bucket")
        self.assertFalse(settings.disable_checksum_validation)
        self.assertFalse(settings.to_json()["DisableChecksumValidation"])

    def test_backup_s3_settings_to_json_writes_checksum_flag(self):
        settings = S3Settings(bucket_name="bucket", disable_checksum_validation=True)
        payload = settings.to_json()
        self.assertTrue(payload["DisableChecksumValidation"])

    def test_backup_s3_settings_from_json_round_trip(self):
        parsed = S3Settings.from_json(
            {
                "Disabled": False,
                "GetBackupConfigurationScript": {
                    "Exec": None,
                    "Arguments": None,
                    "TimeoutInMs": 10000,
                },
                "BucketName": "bucket",
                "DisableChecksumValidation": True,
            }
        )
        self.assertTrue(parsed.disable_checksum_validation)


class TestPullReplicationCursorFields(unittest.TestCase):
    def test_sink_task_info_from_json_reads_cursors(self):
        task = OngoingTaskPullReplicationAsSink.from_json(
            {
                "TaskId": 1,
                "TaskName": "sink",
                "TaskType": "PullReplicationAsSink",
                "HubName": "hub",
                "HubCursor": "hub-cursor-value",
                "SinkCursor": "sink-cursor-value",
            }
        )
        self.assertIsNotNone(task)
        self.assertEqual("hub-cursor-value", task.hub_cursor)
        self.assertEqual("sink-cursor-value", task.sink_cursor)
        self.assertEqual(OngoingTaskType.PULL_REPLICATION_AS_SINK, task.task_type)

    def test_sink_task_info_to_json_writes_cursors(self):
        task = OngoingTaskPullReplicationAsSink(task_id=1, hub_name="hub")
        task.hub_cursor = "hub-cursor-value"
        task.sink_cursor = "sink-cursor-value"
        payload = task.to_json()
        self.assertEqual("hub-cursor-value", payload["HubCursor"])
        self.assertEqual("sink-cursor-value", payload["SinkCursor"])

    def test_sink_task_info_without_cursors(self):
        task = OngoingTaskPullReplicationAsSink.from_json({"TaskId": 1, "TaskName": "sink"})
        self.assertIsNone(task.hub_cursor)
        self.assertIsNone(task.sink_cursor)
