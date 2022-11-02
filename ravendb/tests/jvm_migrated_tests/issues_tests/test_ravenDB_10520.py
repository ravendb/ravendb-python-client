from ravendb.infrastructure.orders import Company
from ravendb.tests.jvm_migrated_tests.issues_tests.test_ravenDB_9745 import Companies_ByName
from ravendb.tests.test_base import TestBase


class CompanyName:
    def __init__(self, name: str = None):
        self.name = name


class TestRavenDB10520(TestBase):
    def setUp(self):
        super(TestRavenDB10520, self).setUp()

    def test_query_can_return_result_as_array(self):
        Companies_ByName().execute(self.store)
        with self.store.open_session() as session:
            company1 = Company()
            company1.name = "Micro"

            company2 = Company()
            company2.name = "Microsoft"

            session.store(company1)
            session.store(company2)

            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            companies = list(session.advanced.document_query(object_type=Company).search("name", "Micro*"))
            self.assertEqual(2, len(companies))

        with self.store.open_session() as session:
            companies = list(session.query(object_type=Company))
            self.assertEqual(2, len(companies))

        with self.store.open_session() as session:
            companies = list(session.query(object_type=Company).select_fields(CompanyName))
            self.assertEqual(2, len(companies))
