"""
Tests for the S3 settings the 7.2.5 patch touched: the DisableChecksumValidation flag on
both settings classes, and the conversions between periodic-backup and remote-attachment
settings that the patch kept in step.
"""

import unittest

from ravendb.documents.operations.attachments import RemoteAttachmentsAzureSettings, RemoteAttachmentsS3Settings
from ravendb.documents.operations.backups.settings import (
    AzureSettings,
    GetBackupConfigurationScript,
    S3Settings,
    S3StorageClass,
)


def _remote_settings() -> RemoteAttachmentsS3Settings:
    return RemoteAttachmentsS3Settings(
        aws_access_key="key",
        aws_secret_key="secret",
        aws_session_token="token",
        aws_region_name="eu-central-1",
        remote_folder_name="attachments",
        bucket_name="bucket",
        custom_server_url="https://minio.local",
        force_path_style=True,
        disable_checksum_validation=True,
        storage_class=S3StorageClass.GLACIER,
    )


class TestDisableChecksumValidation(unittest.TestCase):
    def test_remote_attachment_settings_carry_the_flag(self):
        serialized = _remote_settings().to_json()

        self.assertTrue(serialized["DisableChecksumValidation"])
        self.assertTrue(RemoteAttachmentsS3Settings.from_json(serialized).disable_checksum_validation)

    def test_backup_settings_carry_the_flag(self):
        settings = S3Settings(bucket_name="bucket", disable_checksum_validation=True)

        self.assertTrue(settings.to_json()["DisableChecksumValidation"])
        self.assertTrue(S3Settings.from_json(settings.to_json()).disable_checksum_validation)

    def test_the_flag_is_absent_rather_than_true_by_default(self):
        # Checksum validation protects data integrity, so opting out has to be deliberate.
        self.assertIsNone(S3Settings(bucket_name="bucket").to_json()["DisableChecksumValidation"])
        self.assertIsNone(RemoteAttachmentsS3Settings(bucket_name="bucket").to_json()["DisableChecksumValidation"])


class TestBackupSettingsStorageClass(unittest.TestCase):
    def test_a_chosen_storage_class_is_sent(self):
        settings = S3Settings(bucket_name="bucket", storage_class=S3StorageClass.GLACIER)

        self.assertEqual("Glacier", settings.to_json()["StorageClass"])

    def test_an_unset_storage_class_is_left_to_the_server(self):
        self.assertNotIn("StorageClass", S3Settings(bucket_name="bucket").to_json())

    def test_storage_class_round_trips(self):
        settings = S3Settings(bucket_name="bucket", storage_class=S3StorageClass.STANDARD)

        self.assertEqual(S3StorageClass.STANDARD, S3Settings.from_json(settings.to_json()).storage_class)


class TestBackupSettingsWithoutAScript(unittest.TestCase):
    def test_settings_without_a_configuration_script_serialize(self):
        # C# writes GetBackupConfigurationScript?.ToJson(), so an absent script is normal.
        self.assertIsNone(S3Settings(bucket_name="bucket").to_json()["GetBackupConfigurationScript"])

    def test_settings_without_a_configuration_script_parse(self):
        settings = S3Settings.from_json({"Disabled": False, "BucketName": "bucket"})

        self.assertEqual("bucket", settings.bucket_name)
        self.assertIsNone(settings.get_backup_configuration_script)

    def test_a_script_still_round_trips(self):
        settings = S3Settings(bucket_name="bucket", get_backup_configuration_script=GetBackupConfigurationScript("run"))

        self.assertEqual("run", S3Settings.from_json(settings.to_json()).get_backup_configuration_script.exec)


class TestS3SettingsConversions(unittest.TestCase):
    def test_remote_settings_become_backup_settings(self):
        backup = _remote_settings().to_s3_settings()

        self.assertIsInstance(backup, S3Settings)
        self.assertEqual("bucket", backup.bucket_name)
        self.assertEqual("eu-central-1", backup.aws_region_name)
        self.assertTrue(backup.force_path_style)
        self.assertTrue(backup.disable_checksum_validation)
        self.assertEqual(S3StorageClass.GLACIER, backup.storage_class)

    def test_the_conversion_enables_the_settings_for_direct_upload(self):
        self.assertFalse(_remote_settings().to_s3_settings().disabled)

    def test_a_bucketless_remote_setting_converts_to_nothing(self):
        # The bucket is the minimum the server needs, so there is nothing to convert.
        self.assertIsNone(RemoteAttachmentsS3Settings(aws_access_key="key").to_s3_settings())
        self.assertIsNone(RemoteAttachmentsS3Settings(bucket_name="   ").to_s3_settings())

    def test_backup_settings_become_remote_settings(self):
        remote = S3Settings(
            bucket_name="bucket",
            aws_access_key="key",
            disable_checksum_validation=True,
            storage_class=S3StorageClass.GLACIER,
        ).to_remote_attachments_s3_settings()

        self.assertIsInstance(remote, RemoteAttachmentsS3Settings)
        self.assertEqual("bucket", remote.bucket_name)
        self.assertTrue(remote.disable_checksum_validation)
        self.assertEqual(S3StorageClass.GLACIER, remote.storage_class)

    def test_a_conversion_round_trip_keeps_every_shared_field(self):
        original = _remote_settings()

        self.assertEqual(original.to_json(), original.to_s3_settings().to_remote_attachments_s3_settings().to_json())

    def test_backup_only_fields_are_dropped_on_the_way_out(self):
        backup = S3Settings(
            bucket_name="bucket",
            disabled=True,
            get_backup_configuration_script=GetBackupConfigurationScript("run"),
        )
        remote = backup.to_remote_attachments_s3_settings()

        self.assertNotIn("Disabled", remote.to_json())
        self.assertNotIn("GetBackupConfigurationScript", remote.to_json())


class TestAzureSettingsConversions(unittest.TestCase):
    def _remote(self) -> RemoteAttachmentsAzureSettings:
        return RemoteAttachmentsAzureSettings(
            storage_container="container",
            remote_folder_name="attachments",
            account_name="account",
            account_key="key",
            sas_token="token",
        )

    def test_remote_settings_become_backup_settings(self):
        backup = self._remote().to_azure_settings()

        self.assertIsInstance(backup, AzureSettings)
        self.assertEqual("container", backup.storage_container)
        self.assertEqual("account", backup.account_name)
        self.assertFalse(backup.disabled)

    def test_a_containerless_remote_setting_converts_to_nothing(self):
        self.assertIsNone(RemoteAttachmentsAzureSettings(account_name="account").to_azure_settings())
        self.assertIsNone(RemoteAttachmentsAzureSettings(storage_container="  ").to_azure_settings())

    def test_backup_settings_become_remote_settings(self):
        remote = AzureSettings(
            storage_container="container", account_name="account"
        ).to_remote_attachments_azure_settings()

        self.assertIsInstance(remote, RemoteAttachmentsAzureSettings)
        self.assertEqual("container", remote.storage_container)

    def test_a_conversion_round_trip_keeps_every_shared_field(self):
        original = self._remote()

        self.assertEqual(
            original.to_json(), original.to_azure_settings().to_remote_attachments_azure_settings().to_json()
        )

    def test_backup_only_fields_are_dropped_on_the_way_out(self):
        backup = AzureSettings(
            storage_container="container",
            disabled=True,
            get_backup_configuration_script=GetBackupConfigurationScript("run"),
        )
        remote = backup.to_remote_attachments_azure_settings()

        self.assertNotIn("Disabled", remote.to_json())
        self.assertNotIn("GetBackupConfigurationScript", remote.to_json())

    def test_partial_backup_settings_parse(self):
        self.assertEqual("container", AzureSettings.from_json({"StorageContainer": "container"}).storage_container)
