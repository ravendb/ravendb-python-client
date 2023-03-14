from ravendb.tests.test_base import TestBase, Company


class TestRavenDB15080(TestBase):
    def setUp(self):
        super(TestRavenDB15080, self).setUp()

    def test_can_delete_and_re_insert_counter(self):
        with self.store.open_session() as session:
            company = Company(name="HR")
            session.store(company, "companies/1")
            session.counters_for_entity(company).increment("Likes", 999)
            session.counters_for_entity(company).increment("Cats", 999)
            session.save_changes()

        with self.store.open_session() as session:
            company = session.load("companies/1", Company)
            session.counters_for_entity(company).delete("Likes")
            session.save_changes()

        with self.store.open_session() as session:
            company = session.load("companies/1", Company)
            counters = session.advanced.get_counters_for(company)
            self.assertEqual(1, len(counters))
            self.assertIn("Cats", counters)

            counter = session.counters_for_entity(company).get("Likes")
            self.assertIsNone(counter)

            all = session.counters_for_entity(company).get_all()

            self.assertEqual(1, len(all))

        with self.store.open_session() as session:
            session.counters_for("companies/1").increment("Likes")
            session.save_changes()

        with self.store.open_session() as session:
            company = session.load("companies/1", Company)
            counters = session.advanced.get_counters_for(company)

            self.assertEqual(2, len(counters))
            self.assertSequenceContainsElements(counters, "Cats", "Likes")

            counter = session.counters_for_entity(company).get("Likes")
            self.assertIsNotNone(counter)
            self.assertEqual(1, counter)
