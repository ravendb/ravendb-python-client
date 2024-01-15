from ravendb import CreateDatabaseOperation, GetDatabaseRecordOperation
from ravendb.serverwide.database_record import DatabaseRecord
from ravendb.serverwide.operations.common import DeleteDatabaseOperation
from ravendb.serverwide.operations.ongoing_tasks import SetDatabasesLockOperation
from ravendb.tests.test_base import TestBase


class TestRavenDB16367(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_lock_database(self):
        database_name1 = self.store.database + "_LockMode_1"

        with self.assertRaises(RuntimeError):
            lock_operation = SetDatabasesLockOperation(
                database_name1, DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_ERROR
            )
            self.store.maintenance.server.send(lock_operation)

        self.store.maintenance.server.send(CreateDatabaseOperation(DatabaseRecord(database_name1)))

        database_record = self.store.maintenance.server.send(GetDatabaseRecordOperation(database_name1))
        self.assertEqual(DatabaseRecord.DatabaseLockMode.UNLOCK, database_record.lock_mode)

        self.store.maintenance.server.send(
            SetDatabasesLockOperation(database_name1, DatabaseRecord.DatabaseLockMode.UNLOCK)
        )

        database_record = self.store.maintenance.server.send(GetDatabaseRecordOperation(database_name1))
        self.assertEqual(DatabaseRecord.DatabaseLockMode.UNLOCK, database_record.lock_mode)

        self.store.maintenance.server.send(
            SetDatabasesLockOperation(database_name1, DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_ERROR)
        )

        database_record = self.store.maintenance.server.send(GetDatabaseRecordOperation(database_name1))
        self.assertEqual(DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_ERROR, database_record.lock_mode)

        self.assertRaisesWithMessageContaining(
            self.store.maintenance.server.send,
            Exception,
            "cannot be deleted because of the set lock mode",
            DeleteDatabaseOperation(database_name1, True),
        )

        self.store.maintenance.server.send(
            SetDatabasesLockOperation(database_name1, DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_IGNORE)
        )

        database_record = self.store.maintenance.server.send(GetDatabaseRecordOperation(database_name1))
        self.assertEqual(DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_IGNORE, database_record.lock_mode)

        result = self.store.maintenance.server.send(DeleteDatabaseOperation(database_name1, True))
        self.assertEqual(-1, result.raft_command_index)

        database_record = self.store.maintenance.server.send(GetDatabaseRecordOperation(database_name1))
        self.assertIsNotNone(database_record)
