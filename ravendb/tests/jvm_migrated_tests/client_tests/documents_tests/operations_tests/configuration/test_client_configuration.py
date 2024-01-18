from ravendb.documents.operations.configuration.definitions import ClientConfiguration
from ravendb.documents.operations.configuration.operations import (
    PutServerWideClientConfigurationOperation,
    GetServerWideClientConfigurationOperation,
    GetClientConfigurationOperation,
    PutClientConfigurationOperation,
)
from ravendb.http.misc import ReadBalanceBehavior, LoadBalanceBehavior
from ravendb.tests.test_base import TestBase


class ClientConfigurationTest(TestBase):
    def setUp(self):
        super(ClientConfigurationTest, self).setUp()

    def test_can_save_and_read_server_wide_client_configuration(self):
        configuration_to_save = ClientConfiguration()
        configuration_to_save.max_number_of_requests_per_session = 80
        configuration_to_save.read_balance_behavior = ReadBalanceBehavior.FASTEST_NODE
        configuration_to_save.disabled = True
        configuration_to_save.load_balance_behavior = LoadBalanceBehavior.NONE
        configuration_to_save.load_balancer_context_seed = 0

        save_operation = PutServerWideClientConfigurationOperation(configuration_to_save)
        self.store.maintenance.server.send(save_operation)

        operation = GetServerWideClientConfigurationOperation()
        new_configuration: ClientConfiguration = self.store.maintenance.server.send(operation)

        self.assertIsNotNone(new_configuration)
        self.assertTrue(new_configuration.disabled)

        self.assertEqual(80, new_configuration.max_number_of_requests_per_session)
        self.assertEqual(0, new_configuration.load_balancer_context_seed)
        self.assertEqual(LoadBalanceBehavior.NONE, new_configuration.load_balance_behavior)
        self.assertEqual(ReadBalanceBehavior.FASTEST_NODE, new_configuration.read_balance_behavior)

    def test_can_handle_no_configuration(self):
        operation = GetClientConfigurationOperation()
        result = self.store.maintenance.send(operation)

        self.assertIsNotNone(result.etag)

    def test_can_save_and_read_client_configuration(self):
        configuration_to_save = ClientConfiguration()
        configuration_to_save.etag = 123
        configuration_to_save.max_number_of_requests_per_session = 80
        configuration_to_save.read_balance_behavior = ReadBalanceBehavior.FASTEST_NODE
        configuration_to_save.disabled = True
        save_operation = PutClientConfigurationOperation(configuration_to_save)
        self.store.maintenance.send(save_operation)

        operation = GetClientConfigurationOperation()
        result = self.store.maintenance.send(operation)

        self.assertIsNotNone(result.etag)
        new_configuration: ClientConfiguration = result.configuration

        self.assertIsNotNone(new_configuration)
        self.assertTrue(new_configuration.disabled)
        self.assertEqual(80, new_configuration.max_number_of_requests_per_session)
        self.assertEqual(ReadBalanceBehavior.FASTEST_NODE, new_configuration.read_balance_behavior)
