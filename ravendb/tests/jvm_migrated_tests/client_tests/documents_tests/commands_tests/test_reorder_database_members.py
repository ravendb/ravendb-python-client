from ravendb.serverwide.operations.common import ReorderDatabaseMembersOperation
from ravendb.tests.test_base import TestBase


class TestReorderDatabaseMembers(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_send_reorder_command(self):
        self.store.maintenance.server.send(ReorderDatabaseMembersOperation(self.store.database, ["A"]))
