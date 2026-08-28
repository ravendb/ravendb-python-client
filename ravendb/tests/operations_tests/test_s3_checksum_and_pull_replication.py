"""S3 checksum toggle wire tests."""

import unittest

from ravendb.documents.operations.attachments import RemoteAttachmentsS3Settings
from ravendb.documents.operations.backups.settings import S3Settings


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
