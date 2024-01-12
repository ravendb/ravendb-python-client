from datetime import timedelta

from ravendb.serverwide.operations.logs import (
    GetLogsConfigurationOperation,
    GetLogsConfigurationResult,
    LogMode,
    Parameters,
    SetLogsConfigurationOperation,
)
from ravendb.tests.test_base import TestBase


class TestRavenDB11440(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_get_logs_configuration_and_change_mode(self):
        configuration: GetLogsConfigurationResult = self.store.maintenance.server.send(GetLogsConfigurationOperation())
        try:
            if configuration.current_mode == LogMode.NONE:
                mode_to_set = LogMode.INFORMATION
            elif configuration.current_mode == LogMode.OPERATIONS:
                mode_to_set = LogMode.INFORMATION
            elif configuration.current_mode == LogMode.INFORMATION:
                mode_to_set = LogMode.NONE
            else:
                raise RuntimeError(f"Invalid mode: {configuration.current_mode}")

            time = timedelta(days=1000)

            parameters = Parameters(mode_to_set, time)
            set_logs_operation = SetLogsConfigurationOperation(parameters)
            self.store.maintenance.server.send(set_logs_operation)

            configuration2: GetLogsConfigurationResult = self.store.maintenance.server.send(
                GetLogsConfigurationOperation()
            )

            self.assertEqual(mode_to_set, configuration2.current_mode)
            self.assertEqual(time, configuration2.retention_time)
            self.assertEqual(configuration.mode, configuration2.mode)
            self.assertEqual(configuration.path, configuration2.path)
            self.assertEqual(configuration.use_utc_time, configuration2.use_utc_time)

        finally:
            parameters = Parameters(configuration.current_mode, configuration.retention_time)
            self.store.maintenance.server.send(SetLogsConfigurationOperation(parameters))
