from ravendb import SessionOptions, GetIndexesOperation, AbstractIndexCreationTask, IndexDefinition
from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase


class Companies_ByUnknown(AbstractIndexCreationTask):
    def __init__(self):
        super(Companies_ByUnknown, self).__init__()

    def create_index_definition(self) -> IndexDefinition:
        index_definition = IndexDefinition()
        index_definition.name = "Companies/ByUnknown"
        index_definition.maps = {"from c in docs.Companies select new { unknown = c.unknown };"}
        return index_definition


class Companies_ByUnknown_WithIndexMissingFieldsAsNull(AbstractIndexCreationTask):
    def __init__(self):
        super(Companies_ByUnknown_WithIndexMissingFieldsAsNull, self).__init__()

    def create_index_definition(self) -> IndexDefinition:
        index_definition = IndexDefinition()
        index_definition.name = "Companies/ByUnknown/WithIndexMissingFieldsAsNull"
        index_definition.maps = {"from c in docs.Companies select new { unknown = c.unknown };"}
        index_definition.configuration["Indexing.IndexMissingFieldsAsNull"] = "true"
        return index_definition


class TestRavenDB14084(TestBase):
    def setUp(self):
        super(TestRavenDB14084, self).setUp()

    def test_can_index_missing_fields_an_null_static(self):
        Companies_ByUnknown().execute(self.store)
        Companies_ByUnknown_WithIndexMissingFieldsAsNull().execute(self.store)

        with self.store.open_session() as session:
            company = Company()
            company.name = "HR"
            session.store(company)

            session.save_changes()

        self.wait_for_indexing(self.store)

        session_options = SessionOptions()
        session_options.no_caching = True

        with self.store.open_session(session_options=session_options) as session:
            companies = list(
                session.advanced.document_query_from_index_type(
                    Companies_ByUnknown_WithIndexMissingFieldsAsNull, Company
                ).where_equals("unknown", None)
            )

            self.assertEqual(1, len(companies))

        index_definitions = self.store.maintenance.send(GetIndexesOperation(0, 10))
        self.assertEqual(2, len(index_definitions))

        configuration = (
            list(filter(lambda x: x.name == "Companies/ByUnknown/WithIndexMissingFieldsAsNull", index_definitions))
            .pop()
            .configuration
        )

        self.assertEqual(1, len(configuration))
        self.assertIn("Indexing.IndexMissingFieldsAsNull", configuration)
        self.assertEqual("true", configuration["Indexing.IndexMissingFieldsAsNull"])
