from ravendb.serverwide.commands import GetTcpInfoCommand
from ravendb.tests.test_base import TestBase


class TestGetTcpInfo(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_get_tcp_info(self):
        command = GetTcpInfoCommand("test")
        self.store.get_request_executor().execute_command(command)
        result = command.result

        self.assertIsNotNone(result)
        self.assertIsNone(result.certificate)
        self.assertIsNotNone(result.port)
        self.assertIsNotNone(result.url)
