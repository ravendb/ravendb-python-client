"""
Tests for DisableChecksumValidation, added in 7.2.5 to both S3 settings classes for
S3-compatible storage that does not support modern object integrity checks.
"""

import unittest

from ravendb.documents.operations.attachments import RemoteAttachmentsS3Settings
from ravendb.documents.operations.backups.settings import GetBackupConfigurationScript, S3Settings


class TestBackupS3Settings(unittest.TestCase):
    def _settings(self, **kwargs) -> S3Settings:
        return S3Settings(
            disabled=False,
            get_backup_configuration_script=GetBackupConfigurationScript(),
            aws_access_key="key",
            aws_secret_key="secret",
            aws_session_token=None,
            aws_region_name="eu-central-1",
            remote_folder_name="backups",
            bucket_name="rvn",
            custom_server_url="https://minio.local",
            force_path_style=True,
            **kwargs,
        )

    def test_checksum_validation_is_left_alone_by_default(self):
        self.assertIsNone(self._settings().disable_checksum_validation)

    def test_the_flag_is_written(self):
        self.assertTrue(self._settings(disable_checksum_validation=True).to_json()["DisableChecksumValidation"])

    def test_the_flag_round_trips(self):
        serialized = self._settings(disable_checksum_validation=True).to_json()

        self.assertEqual(serialized, S3Settings.from_json(serialized).to_json())

    def test_settings_from_a_pre_7_2_5_server_parse_without_the_flag(self):
        serialized = self._settings().to_json()
        del serialized["DisableChecksumValidation"]

        self.assertIsNone(S3Settings.from_json(serialized).disable_checksum_validation)


class TestRemoteAttachmentsS3Settings(unittest.TestCase):
    def _settings(self, **kwargs) -> RemoteAttachmentsS3Settings:
        return RemoteAttachmentsS3Settings(
            aws_access_key="key",
            aws_secret_key="secret",
            aws_region_name="eu-central-1",
            bucket_name="rvn",
            force_path_style=True,
            **kwargs,
        )

    def test_checksum_validation_is_left_alone_by_default(self):
        self.assertIsNone(self._settings().disable_checksum_validation)

    def test_the_flag_is_written(self):
        self.assertTrue(self._settings(disable_checksum_validation=True).to_json()["DisableChecksumValidation"])

    def test_the_flag_round_trips(self):
        serialized = self._settings(disable_checksum_validation=True).to_json()

        self.assertEqual(serialized, RemoteAttachmentsS3Settings.from_json(serialized).to_json())

    def test_settings_from_a_pre_7_2_5_server_parse_without_the_flag(self):
        self.assertIsNone(RemoteAttachmentsS3Settings.from_json({"BucketName": "rvn"}).disable_checksum_validation)
