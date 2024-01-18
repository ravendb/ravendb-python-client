from ravendb.documents.operations.compact import CompactDatabaseOperation
from ravendb.infrastructure.entities import User
from ravendb.serverwide.misc import CompactSettings
from ravendb.tests.test_base import TestBase


class TestCompact(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_compact_database(self):
        with self.store.open_session() as new_session:
            user1 = User()
            user1.last_name = "user1"
            new_session.store(user1, "users/1")
            new_session.save_changes()

        compact_settings = CompactSettings()
        compact_settings.database_name = self.store.database
        compact_settings.documents = True

        operation = self.store.maintenance.server.send_async(CompactDatabaseOperation(compact_settings))

        # we can't compact in memory database but here we just test if request was send successfully
        self.assertRaisesWithMessageContaining(
            operation.wait_for_completion,
            Exception,
            "Unable to cast object of type 'PureMemoryStorageEnvironmentOptions' to type 'DirectoryStorageEnvironmentOptions'",
        )
