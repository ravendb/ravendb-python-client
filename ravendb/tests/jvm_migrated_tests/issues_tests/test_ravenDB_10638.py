from ravendb.documents.queries.query import QueryResult

from ravendb.tests.test_base import TestBase, User


class TestRavenDB10638(TestBase):
    def setUp(self):
        super(TestRavenDB10638, self).setUp()

    def test_after_query_executed_should_be_executed_only_once(self):
        with self.store.open_session() as session:
            counter = 0

            def on_after_query_executed(event_args: QueryResult):
                nonlocal counter
                counter += 1

            results = list(
                session.query(object_type=User)
                .add_after_query_executed_listener(on_after_query_executed)
                .where_equals("name", "Doe")
            )

            self.assertEqual(0, len(results))
            self.assertEqual(1, counter)