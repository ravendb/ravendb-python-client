import unittest
from typing import List

from ravendb import FtpSettings, AzureSettings, GetDatabaseRecordOperation, CreateDatabaseOperation, DocumentStore
from ravendb.documents.operations.backups.settings import BackupType
from ravendb.documents.operations.ongoing_tasks import OngoingTaskType
from ravendb.serverwide.database_record import DatabaseRecordWithEtag, DatabaseRecord
from ravendb.serverwide.operations.configuration import (
    ServerWideBackupConfiguration,
    PutServerWideBackupConfigurationOperation,
    GetServerWideBackupConfigurationsOperation,
    GetServerWideBackupConfigurationOperation,
    DeleteServerWideTaskOperation,
)
from ravendb.tests.test_base import TestBase


class TestServerWideBackup(TestBase):
    def setUp(self):
        super().setUp()

    @unittest.skip("Skipping due to license on CI/CD")
    def test_can_crud_server_wide_backup(self):
        try:
            put_configuration = ServerWideBackupConfiguration()
            put_configuration.disabled = True
            put_configuration.full_backup_frequency = " 0 2 * * 0"
            put_configuration.incremental_backup_frequency = "0 2 * * 1"

            self.store.maintenance.server.send(PutServerWideBackupConfigurationOperation(put_configuration))

            ftp_settings = FtpSettings()
            ftp_settings.url = "http://url:8080"
            ftp_settings.disabled = True

            put_configuration.ftp_settings = ftp_settings
            self.store.maintenance.server.send(PutServerWideBackupConfigurationOperation(put_configuration))

            azure_settings = AzureSettings()
            azure_settings.disabled = True
            azure_settings.account_key = "test"

            put_configuration.azure_settings = azure_settings
            self.store.maintenance.server.send(PutServerWideBackupConfigurationOperation(put_configuration))

            server_wide_backups: List[ServerWideBackupConfiguration] = self.store.maintenance.server.send(
                GetServerWideBackupConfigurationsOperation()
            )

            self.assertEqual(3, len(server_wide_backups))

            database_record: DatabaseRecordWithEtag = self.store.maintenance.server.send(
                GetDatabaseRecordOperation(self.store.database)
            )
            self.assertEqual(3, len(database_record.periodic_backups))

            # update one of the tasks
            to_update = server_wide_backups[1]
            to_update.backup_type = BackupType.SNAPSHOT
            self.store.maintenance.server.send(PutServerWideBackupConfigurationOperation(to_update))

            server_wide_backups = self.store.maintenance.server.send(GetServerWideBackupConfigurationsOperation())
            self.assertEqual(3, len(server_wide_backups))

            database_record = self.store.maintenance.server.send(GetDatabaseRecordOperation(self.store.database))
            self.assertEqual(3, len(database_record.periodic_backups))

            # new database includes all server-wide backups
            new_db_name = self.store.database + "-testDatabase"
            self.store.maintenance.server.send(CreateDatabaseOperation(DatabaseRecord(new_db_name)))
            database_record = self.store.maintenance.server.send(GetDatabaseRecordOperation(new_db_name))
            self.assertEqual(3, len(database_record.periodic_backups))

            # get by name
            backup_configuration = self.store.maintenance.server.send(
                GetServerWideBackupConfigurationOperation("Backup w/o destinations")
            )

            self.assertIsNotNone(backup_configuration)
        finally:
            cleanup_server_wide_backups(self.store)


def cleanup_server_wide_backups(store: DocumentStore) -> None:
    backup_configurations = store.maintenance.server.send(GetServerWideBackupConfigurationsOperation())
    names = [x.name for x in backup_configurations]

    for name in names:
        store.maintenance.server.send(DeleteServerWideTaskOperation(name, OngoingTaskType.BACKUP))
