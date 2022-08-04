from ravendb import (
    SuggestionOptions,
    StringDistanceTypes,
    SuggestionSortMode,
    DocumentStore,
    IndexDefinition,
    IndexFieldOptions,
    PutIndexesOperation,
)
from ravendb.tests.jvm_migrated_tests.client_tests.indexing_tests.test_indexes_from_client import Users_ByName
from ravendb.tests.test_base import TestBase


class User:
    def __init__(self, Id: str = None, name: str = None, email: str = None):
        self.Id = Id
        self.name = name
        self.email = email


class TestSuggestions(TestBase):
    def setUp(self):
        super(TestSuggestions, self).setUp()

    def set_up(self, store: DocumentStore) -> None:
        index_definition = IndexDefinition()
        index_definition.name = "test"
        index_definition.maps = ["from doc in docs.Users select new { doc.name }"]
        index_field_options = IndexFieldOptions()
        index_field_options.suggestions = True
        index_definition.fields = {"name": index_field_options}

        store.maintenance.send(PutIndexesOperation(index_definition))

        with self.store.open_session() as session:
            user1 = User(name="Ayende")
            user2 = User(name="Oren")
            user3 = User(name="John Steinbeck")

            session.store(user1)
            session.store(user2)
            session.store(user3)
            session.save_changes()

        self.wait_for_indexing(store)

    def test_can_get_suggestions(self):
        Users_ByName().execute(self.store)

        with self.store.open_session() as s:
            user1 = User(name="John Smith")
            s.store(user1, "users/1")

            user2 = User(name="Jack Johnson")
            s.store(user2, "users/2")

            user3 = User(name="Robery Jones")
            s.store(user3, "users/3")

            user4 = User(name="David Jones")
            s.store(user4, "users/4")

            s.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            options = SuggestionOptions(
                accuracy=0.4,
                page_size=5,
                distance=StringDistanceTypes.JARO_WINKLER,
                sort_mode=SuggestionSortMode.POPULARITY,
            )

            suggestions = (
                session.query_index_type(Users_ByName, User)
                .suggest_using(lambda x: x.by_field("name", ["johne", "davi"]).with_options(options))
                .execute()
            )

            self.assertEqual(5, len(suggestions.get("name").suggestions))
            self.assertSequenceContainsElements(
                suggestions.get("name").suggestions, "john", "jones", "johnson", "david", "jack"
            )

    def test_using_linq_multiple_words(self):
        self.set_up(self.store)

        with self.store.open_session() as s:

            options = SuggestionOptions(accuracy=0.4, distance=StringDistanceTypes.LEVENSHTEIN)

            suggestion_query_result = (
                s.query_index("test", User)
                .suggest_using(lambda x: x.by_field("name", "John Steinback").with_options(options))
                .execute()
            )

            self.assertEqual(1, len(suggestion_query_result.get("name").suggestions))
            self.assertEqual("john steinbeck", suggestion_query_result.get("name").suggestions[0])

    def test_with_typo(self):
        self.set_up(self.store)

        with self.store.open_session() as s:
            options = SuggestionOptions(accuracy=0.2, page_size=10, distance=StringDistanceTypes.LEVENSHTEIN)
            suggestion_query_result = (
                s.query_index("test", User)
                .suggest_using(lambda x: x.by_field("name", "Oern").with_options(options))
                .execute()
            )

            self.assertEqual(1, len(suggestion_query_result.get("name").suggestions))
            self.assertEqual("oren", suggestion_query_result.get("name").suggestions[0])

    def test_using_linq(self):
        self.set_up(self.store)

        with self.store.open_session() as s:
            suggestion_query_result = (
                s.query_index("test", User).suggest_using(lambda x: x.by_field("name", "Owen")).execute()
            )
            self.assertEqual(1, len(suggestion_query_result.get("name").suggestions))
            self.assertEqual("oren", suggestion_query_result.get("name").suggestions[0])
