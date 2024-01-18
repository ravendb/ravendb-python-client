from ravendb import CreateDatabaseOperation, GetDatabaseRecordOperation, DocumentStore
from ravendb.documents.operations.server_misc import ToggleDatabasesStateOperation
from ravendb.serverwide.database_record import DatabaseRecord, DatabaseRecordWithEtag
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

    def test_can_lock_database_disabled(self):
        database_name = self.store.database + "_LockMode_1"

        self.store.maintenance.server.send(CreateDatabaseOperation(DatabaseRecord(database_name)))

        self._assert_lock_mode(self.store, database_name, DatabaseRecord.DatabaseLockMode.UNLOCK)

        self.store.maintenance.server.send(ToggleDatabasesStateOperation(database_name, True))

        self.store.maintenance.server.send(
            SetDatabasesLockOperation(database_name, DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_ERROR)
        )

        self._assert_lock_mode(self.store, database_name, DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_ERROR)

        self.store.maintenance.server.send(
            SetDatabasesLockOperation(database_name, DatabaseRecord.DatabaseLockMode.UNLOCK)
        )

        self.store.maintenance.server.send(DeleteDatabaseOperation(database_name, True))

    def test_can_lock_database_multiple(self):
        database_name1 = self.store.database + "_LockMode_1"
        database_name2 = self.store.database + "_LockMode_2"
        database_name3 = self.store.database + "_LockMode_3"

        databases = [database_name1, database_name2, database_name3]

        with self.assertRaises(RuntimeError):
            parameters = SetDatabasesLockOperation.Parameters()
            parameters.database_names = [databases]
            parameters.mode = DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_ERROR
            self.store.maintenance.server.send(SetDatabasesLockOperation.from_parameters(parameters))

        self.store.maintenance.server.send(CreateDatabaseOperation(DatabaseRecord(database_name1)))

        with self.assertRaises(RuntimeError):
            parameters = SetDatabasesLockOperation.Parameters()
            parameters.database_names = databases
            parameters.mode = DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_ERROR
            self.store.maintenance.server.send(SetDatabasesLockOperation.from_parameters(parameters))

        self._assert_lock_mode(self.store, database_name1, DatabaseRecord.DatabaseLockMode.UNLOCK)

        self.store.maintenance.server.send(CreateDatabaseOperation(DatabaseRecord(database_name2)))
        self.store.maintenance.server.send(CreateDatabaseOperation(DatabaseRecord(database_name3)))

        self._assert_lock_mode(self.store, database_name2, DatabaseRecord.DatabaseLockMode.UNLOCK)
        self._assert_lock_mode(self.store, database_name3, DatabaseRecord.DatabaseLockMode.UNLOCK)

        p2 = SetDatabasesLockOperation.Parameters()
        p2.database_names = databases
        p2.mode = DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_ERROR
        self.store.maintenance.server.send(SetDatabasesLockOperation.from_parameters(p2))

        self._assert_lock_mode(self.store, database_name1, DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_ERROR)
        self._assert_lock_mode(self.store, database_name2, DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_ERROR)
        self._assert_lock_mode(self.store, database_name3, DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_ERROR)

        self.store.maintenance.server.send(
            SetDatabasesLockOperation(database_name2, DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_IGNORE)
        )

        self._assert_lock_mode(self.store, database_name1, DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_ERROR)
        self._assert_lock_mode(self.store, database_name2, DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_IGNORE)
        self._assert_lock_mode(self.store, database_name3, DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_ERROR)

        p2.database_names = databases
        p2.mode = DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_IGNORE

        self.store.maintenance.server.send(SetDatabasesLockOperation.from_parameters(p2))

        self._assert_lock_mode(self.store, database_name1, DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_IGNORE)
        self._assert_lock_mode(self.store, database_name2, DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_IGNORE)
        self._assert_lock_mode(self.store, database_name3, DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_IGNORE)

        p2.database_names = databases
        p2.mode = DatabaseRecord.DatabaseLockMode.UNLOCK

        self.store.maintenance.server.send(SetDatabasesLockOperation.from_parameters(p2))

        self.store.maintenance.server.send(DeleteDatabaseOperation(database_name1, True))
        self.store.maintenance.server.send(DeleteDatabaseOperation(database_name2, True))
        self.store.maintenance.server.send(DeleteDatabaseOperation(database_name3, True))

    def _assert_lock_mode(
        self, store: DocumentStore, database_name: str, mode: DatabaseRecord.DatabaseLockMode
    ) -> None:
        database_record: DatabaseRecordWithEtag = store.maintenance.server.send(
            GetDatabaseRecordOperation(database_name)
        )
        self.assertEqual(mode, database_record.lock_mode)
