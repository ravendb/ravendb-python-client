from ravendb.serverwide.operations.logs import GetLogsConfigurationOperation, LogMode, SetLogsConfigurationOperation
from ravendb.tests.test_base import TestBase


class TestLogsConfiguration(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_get_and_set_logging(self):
        try:
            get_operation = GetLogsConfigurationOperation()

            logs_config = self.store.maintenance.server.send(get_operation)

            self.assertEqual(LogMode.NONE, logs_config.current_mode)

            self.assertEqual(LogMode.NONE, logs_config.mode)

            # now try to set mode to operations and info
            parameters = SetLogsConfigurationOperation.Parameters(LogMode.INFORMATION)
            set_operation = SetLogsConfigurationOperation(parameters)

            self.store.maintenance.server.send(set_operation)

            get_operation = GetLogsConfigurationOperation()

            logs_config = self.store.maintenance.server.send(get_operation)

            self.assertEqual(LogMode.INFORMATION, logs_config.current_mode)

            self.assertEqual(LogMode.NONE, logs_config.mode)
        finally:
            # try to clean up

            parameters = SetLogsConfigurationOperation.Parameters(LogMode.OPERATIONS)
            set_operation = SetLogsConfigurationOperation(parameters)
            self.store.maintenance.server.send(set_operation)
