from ravendb.documents.operations.identities import NextIdentityForOperation
from ravendb.tests.test_base import TestBase


class TestNextAndSeedIdentities(TestBase):
    def setUp(self):
        super().setUp()

    def test_next_identity_for_operation_should_create_a_new_identify_if_there_is_none(self):
        with self.store.open_session() as session:
            result = self.store.maintenance.send(NextIdentityForOperation("person|"))

            self.assertEqual(1, result)
