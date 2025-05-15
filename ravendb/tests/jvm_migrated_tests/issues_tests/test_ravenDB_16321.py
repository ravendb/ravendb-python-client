from ravendb.documents.queries.misc import Query
from ravendb.exceptions.exceptions import IndexDoesNotExistException
from ravendb.tests.test_base import TestBase


class TestRavenDB16321(TestBase):
    def setUp(self):
        super(TestRavenDB16321, self).setUp()

    def test_streaming_on_index_that_does_not_exist_should_throw(self):
        with self.store.open_session() as session:
            query = session.query(Query.from_index_name("idkb2")).where_equals("Natalia", "first_name")

            with self.assertRaises(IndexDoesNotExistException):
                stream = session.advanced.stream(query)
                stream.__next__()
