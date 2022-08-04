from ravendb import SuggestionOptions, StringDistanceTypes, SuggestionSortMode
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
