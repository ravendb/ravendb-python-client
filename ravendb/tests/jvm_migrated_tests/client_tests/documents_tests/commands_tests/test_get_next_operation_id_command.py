from ravendb.documents.commands.bulkinsert import GetNextOperationIdCommand
from ravendb.tests.test_base import TestBase


class TestGetNextOperationIdCommand(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_get_next_operation_id(self):
        command = GetNextOperationIdCommand()
        self.store.get_request_executor().execute_command(command)
        self.assertIsNotNone(command.result)
