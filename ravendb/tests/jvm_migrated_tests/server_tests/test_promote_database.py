from ravendb.serverwide.operations.common import PromoteDatabaseNodeOperation
from ravendb.tests.test_base import TestBase


class TestPromoteDatabase(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_send_promote_database_command(self):
        operation = PromoteDatabaseNodeOperation(self.store.database, "A")
        self.store.maintenance.server.send(operation)

        # since we are running single node cluster we cannot assert much
