from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase


class TestRavenDB12030(TestBase):
    def setUp(self):
        super().setUp()

    def test_simple_fuzzy(self):
        with self.store.open_session() as session:
            hr = Company()
            hr.name = "Hibernating Rhinos"
            session.store(hr)

            cf = Company()
            cf.name = "CodeForge"
            session.store(cf)

            session.save_changes()

        with self.store.open_session() as session:
            companies = list(
                session.advanced.document_query(object_type=Company).where_equals("name", "CoedForhe").fuzzy(0.5)
            )

            self.assertEqual(1, len(companies))
            self.assertEqual("CodeForge", companies[0].name)

            companies = list(
                session.advanced.document_query(object_type=Company)
                .where_equals("name", "Hiberanting Rinhos")
                .fuzzy(0.5)
            )

            self.assertEqual(1, len(companies))
            self.assertEqual("Hibernating Rhinos", companies[0].name)

            companies = list(
                session.advanced.document_query(object_type=Company).where_equals("name", "CoedForhe").fuzzy(0.99)
            )
            self.assertEqual(0, len(companies))
