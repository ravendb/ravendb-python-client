from ravendb.serverwide.operations.logs import (
    GetLogsConfigurationOperation,
    LogLevel,
    SetLogsConfigurationOperation,
)
from ravendb.tests.test_base import TestBase


class TestRavenDB11440(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_get_logs_configuration_and_change_mode(self):
        configuration = self.store.maintenance.server.send(GetLogsConfigurationOperation())
        try:
            current = configuration.logs.current_min_level
            level_to_set = LogLevel.TRACE if current != LogLevel.TRACE else LogLevel.DEBUG

            self.store.maintenance.server.send(
                SetLogsConfigurationOperation(SetLogsConfigurationOperation.LogsConfiguration(level_to_set))
            )

            configuration2 = self.store.maintenance.server.send(GetLogsConfigurationOperation())

            self.assertEqual(level_to_set, configuration2.logs.current_min_level)
            self.assertEqual(configuration.logs.min_level, configuration2.logs.min_level)
            self.assertEqual(configuration.logs.path, configuration2.logs.path)
            self.assertEqual(
                configuration.logs.enable_archive_file_compression,
                configuration2.logs.enable_archive_file_compression,
            )
        finally:
            self.store.maintenance.server.send(
                SetLogsConfigurationOperation(
                    SetLogsConfigurationOperation.LogsConfiguration(configuration.logs.current_min_level)
                )
            )
