from ravendb import IndexDefinition, IndexFieldOptions, PutIndexesOperation
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestSuggestionsLazy(TestBase):
    def setUp(self):
        super(TestSuggestionsLazy, self).setUp()

    def test_using_linq(self):
        index_definition = IndexDefinition()
        index_definition.name = "Test"
        index_definition.maps = ["from doc in docs.Users select new { doc.name }"]
        index_field_options = IndexFieldOptions(suggestions=True)
        index_definition.fields = {"name": index_field_options}

        self.store.maintenance.send(PutIndexesOperation(index_definition))

        with self.store.open_session() as s:
            user1 = User(name="Ayende")
            s.store(user1)

            user2 = User(name="Oren")
            s.store(user2)

            s.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as s:
            old_requests = s.advanced.number_of_requests

            suggestion_query_result = (
                s.query_index("test", User).suggest_using(lambda x: x.by_field("name", "Owen")).execute_lazy()
            )

            self.assertEqual(old_requests, s.advanced.number_of_requests)
            self.assertEqual(1, len(suggestion_query_result.value.get("name").suggestions))
            self.assertEqual("oren", suggestion_query_result.value.get("name").suggestions[0])
            self.assertEqual(old_requests + 1, s.advanced.number_of_requests)
