from ravendb.documents.operations.identities import NextIdentityForOperation, SeedIdentityForOperation
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestNextAndSeedIdentities(TestBase):
    def setUp(self):
        super().setUp()

    def test_next_identity_for_operation_should_create_a_new_identify_if_there_is_none(self):
        with self.store.open_session() as session:
            result = self.store.maintenance.send(NextIdentityForOperation("person|"))

            self.assertEqual(1, result)

    def test_seed_identity_for(self):
        with self.store.open_session() as session:
            user = User(last_name="Adi")
            session.store(user, "users|")
            session.save_changes()

        result1 = self.store.maintenance.send(SeedIdentityForOperation("users", 1990))
        self.assertEqual(1990, result1)

        with self.store.open_session() as session:
            user = User(last_name="Avivi")
            session.store(user, "users|")
            session.save_changes()

        with self.store.open_session() as session:
            entity_with_id_1 = session.load("users/1", User)
            entity_with_id_2 = session.load("users/2", User)
            entity_with_id_1990 = session.load("users/1990", User)
            entity_with_id_1991 = session.load("users/1991", User)
            entity_with_id_1992 = session.load("users/1992", User)

            self.assertIsNotNone(entity_with_id_1)
            self.assertIsNotNone(entity_with_id_1991)
            self.assertIsNone(entity_with_id_2)
            self.assertIsNone(entity_with_id_1990)
            self.assertIsNone(entity_with_id_1992)

            self.assertEqual("Adi", entity_with_id_1.last_name)
            self.assertEqual("Avivi", entity_with_id_1991.last_name)

        result2 = self.store.maintenance.send(SeedIdentityForOperation("users", 1975))
        self.assertEqual(1991, result2)

        result3 = self.store.maintenance.send(SeedIdentityForOperation("users", 1975, True))
        self.assertEqual(1975, result3)
