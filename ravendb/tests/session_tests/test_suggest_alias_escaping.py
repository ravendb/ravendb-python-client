"""
Suggestions: display names containing spaces are quoted in the generated RQL.

C# reference: SlowTests/Issues/RavenDB_20673.cs
  CustomizeDisplayNameWithSpaces, CustomizeDisplayNameWithOutSpaces
"""

from ravendb.documents.queries.suggestions import SuggestionBuilder
from ravendb.tests.test_base import TestBase


class User:
    def __init__(self, name: str = None):
        self.name = name


class TestRavenDB20673(TestBase):
    def setUp(self):
        super().setUp()

    def _setup_data(self):
        with self.store.open_session() as session:
            session.store(User(name="dan"), "users/1")
            session.store(User(name="daniel"), "users/2")
            session.store(User(name="danielle"), "users/3")
            session.save_changes()

        self.wait_for_indexing(self.store)

    def test_suggestion_display_name_without_spaces(self):
        """Display names without spaces must work — baseline sanity check."""
        self._setup_data()

        with self.store.open_session() as session:

            def build(b: SuggestionBuilder):
                b.by_field("name", "daniele").with_display_name("CustomizedName")

            suggestion_query = session.query(object_type=User).suggest_using(build)
            rql = suggestion_query.__str__()
            self.assertIn("CustomizedName", rql)

            results = suggestion_query.execute()
            self.assertIn("CustomizedName", results)
            self.assertEqual(2, len(results["CustomizedName"].suggestions))
            self.assertIn("danielle", results["CustomizedName"].suggestions)

    def test_suggestion_display_name_with_spaces(self):
        """Display names containing spaces must be quoted in the RQL."""
        self._setup_data()

        with self.store.open_session() as session:

            def build(b: SuggestionBuilder):
                b.by_field("name", "daniele").with_display_name("Customized name with spaces")

            suggestion_query = session.query(object_type=User).suggest_using(build)
            rql = suggestion_query.__str__()
            # escape_if_necessary wraps aliases containing spaces in single quotes
            self.assertIn("'Customized name with spaces'", rql)

            results = suggestion_query.execute()
            self.assertIn("Customized name with spaces", results)
            self.assertEqual(2, len(results["Customized name with spaces"].suggestions))
            self.assertIn("danielle", results["Customized name with spaces"].suggestions)
