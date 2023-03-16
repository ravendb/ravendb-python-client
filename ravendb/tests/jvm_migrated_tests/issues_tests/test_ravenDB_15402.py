from ravendb.tests.test_base import TestBase, Company


class TestRavenDB15402(TestBase):
    def setUp(self):
        super(TestRavenDB15402, self).setUp()

    def test_counters_should_be_case_insensitive(self):
        key = "companies/1"

        with self.store.open_session() as session:
            session.store(Company(), key)
            session.counters_for(key).increment("Likes", 999)
            session.counters_for(key).increment("Dislikes", 999)
            session.save_changes()

        with self.store.open_session() as session:
            company = session.load(key, Company)
            counters = session.counters_for(company).get_many(["likes", "dislikes"])
            self.assertEqual(2, len(counters))
            self.assertEqual(999, counters["likes"])
            self.assertEqual(999, counters["dislikes"])
