from pyravendb.serverwide.operations.common import GetDatabaseNamesOperation
from pyravendb.tests.test_base import *
import unittest


class TestServerOperations(TestBase):
    def tearDown(self):
        super(TestServerOperations, self).tearDown()
        TestBase.delete_all_topology_files()

    def test_create_database_name_longer_than_260_chars(self):
        name = "long_database_name_" + "".join(["z" for _ in range(100)])
        try:
            self.store.maintenance.server.send(CreateDatabaseOperation(DatabaseRecord(name)))
            TestBase.wait_for_database_topology(self.store, name)
            database_names = self.store.maintenance.server.send(GetDatabaseNamesOperation(0, 3))
            self.assertTrue(name in database_names)
        finally:
            try:
                self.store.maintenance.server.send(DeleteDatabaseOperation(database_name=name, hard_delete=True))
            except Exception as exception:
                raise exception

    @unittest.skip("Exception dispatcher")
    def test_cannot_create_database_with_the_same_name(self):
        name = "Duplicate"
        try:
            self.store.maintenance.server.send(CreateDatabaseOperation(DatabaseRecord(name)))
            TestBase.wait_for_database_topology(self.store, name)
            self.assertIsNone(self.store.maintenance.server.send(CreateDatabaseOperation(DatabaseRecord(name))))

        finally:
            try:
                self.store.maintenance.server.send(DeleteDatabaseOperation(database_name=name, hard_delete=True))
            except Exception as exception:
                raise exception


if __name__ == "__main__":
    unittest.main()
