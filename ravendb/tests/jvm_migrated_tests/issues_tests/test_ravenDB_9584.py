from ravendb import IndexDefinition, IndexFieldOptions, PutIndexesOperation
from ravendb.tests.test_base import TestBase


class User:
    def __init__(self, Id: str = None, name: str = None, company: str = None, title: str = None):
        self.Id = Id
        self.name = name
        self.company = company
        self.title = title


class TestRavenDB9584(TestBase):
    def setUp(self):
        super(TestRavenDB9584, self).setUp()
        index_definition = IndexDefinition()
        index_definition.name = "test"
        index_definition.maps = {"from doc in docs.Users select new { doc.name, doc.company } "}

        name_index_field_options = IndexFieldOptions(suggestions=True)
        company_index_field_options = IndexFieldOptions(suggestions=True)

        index_definition.fields = {"name": name_index_field_options, "company": company_index_field_options}

        results = self.store.maintenance.send(PutIndexesOperation(index_definition))

        self.assertEqual(1, len(results))
        self.assertEqual(index_definition.name, results["Results"][0]["Index"])

        with self.store.open_session() as session:
            ayende = User(name="Ayende", company="Hibernating")
            oren = User(name="Oren", company="HR")
            john = User(name="John Steinbeck", company="Unknown")

            session.store(ayende)
            session.store(oren)
            session.store(john)
            session.save_changes()

            self.wait_for_indexing(self.store)

    def test_can_use_suggestions_with_auto_index(self):
        with self.store.open_session() as s:
            suggestion_query_result = (
                s.query_index("test", User)
                .suggest_using(lambda x: x.by_field("name", "Owen"))
                .and_suggest_using(lambda x: x.by_field("company", "Hiberanting"))
                .execute()
            )

            self.assertEqual(1, len(suggestion_query_result.get("name").suggestions))
            self.assertEqual("oren", suggestion_query_result.get("name").suggestions[0])
            self.assertEqual(1, len(suggestion_query_result.get("company").suggestions))
            self.assertEqual("hibernating", suggestion_query_result.get("company").suggestions[0])

    def test_can_chain_suggestions(self):
        with self.store.open_session() as s:
            suggestion_query_result = (
                s.query_index("test", User)
                .suggest_using(lambda x: x.by_field("name", "Owen"))
                .and_suggest_using(lambda x: x.by_field("company", "Hiberanting"))
                .execute()
            )

            self.assertEqual(1, len(suggestion_query_result.get("name").suggestions))
            self.assertEqual("oren", suggestion_query_result.get("name").suggestions[0])
            self.assertEqual(1, len(suggestion_query_result.get("company").suggestions))
            self.assertEqual("hibernating", suggestion_query_result.get("company").suggestions[0])
