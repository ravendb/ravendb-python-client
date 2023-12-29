import unittest

from ravendb import ExpirationConfiguration
from ravendb.documents.operations.expiration.operations import ConfigureExpirationOperation
from ravendb.tests.test_base import TestBase


class TestExpirationConfiguration(TestBase):
    def setUp(self):
        super().setUp()

    @unittest.skip("License on ci/cd")
    def test_can_setup_expiration(self):
        expiration_configuration = ExpirationConfiguration(False, 5)
        configure_operation = ConfigureExpirationOperation(expiration_configuration)

        expiration_operation_result = self.store.maintenance.send(configure_operation)

        self.assertIsNotNone(expiration_operation_result.raft_command_index)
