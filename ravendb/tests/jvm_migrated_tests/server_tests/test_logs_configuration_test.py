from ravendb.serverwide.operations.logs import GetLogsConfigurationOperation, LogLevel, SetLogsConfigurationOperation
from ravendb.tests.test_base import TestBase


class TestLogsConfiguration(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_get_and_set_logging(self):
        logs_config = self.store.maintenance.server.send(GetLogsConfigurationOperation())
        initial_current = logs_config.logs.current_min_level
        persisted = logs_config.logs.min_level

        try:
            # change the runtime min level (not persisted)
            self.store.maintenance.server.send(
                SetLogsConfigurationOperation(SetLogsConfigurationOperation.LogsConfiguration(LogLevel.WARN))
            )

            logs_config = self.store.maintenance.server.send(GetLogsConfigurationOperation())
            self.assertEqual(LogLevel.WARN, logs_config.logs.current_min_level)
            # without persist, the persisted MinLevel is unchanged
            self.assertEqual(persisted, logs_config.logs.min_level)
        finally:
            # restore the original runtime level
            self.store.maintenance.server.send(
                SetLogsConfigurationOperation(SetLogsConfigurationOperation.LogsConfiguration(initial_current))
            )
