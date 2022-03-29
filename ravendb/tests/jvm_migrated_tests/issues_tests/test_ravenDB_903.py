from typing import Callable

from ravendb.documents.indexes.definitions import FieldIndexing
from ravendb.documents.indexes.index_creation import AbstractIndexCreationTask
from ravendb.documents.session.document_session import DocumentSession
from ravendb.documents.session.query import DocumentQuery
from ravendb.tests.test_base import TestBase


class Product:
    def __init__(self, name: str = None, description: str = None):
        self.name = name
        self.description = description


class TestIndex(AbstractIndexCreationTask):
    def __init__(self):
        super(TestIndex, self).__init__()
        self.map = "from product in docs.Products select new { product.name, product.description }"
        self._index("description", FieldIndexing.SEARCH)


class TestRavenDB903(TestBase):
    def setUp(self):
        super(TestRavenDB903, self).setUp()

    def do_test(self, query_function: Callable[[DocumentSession], DocumentQuery]):
        self.store.execute_index(TestIndex())

        with self.store.open_session() as session:
            product1 = Product("Foo", "Hello World")
            product2 = Product("Bar", "Hello World")
            product3 = Product("Bar", "Goodbye World")
            session.store(product1)
            session.store(product2)
            session.store(product3)

            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            query = query_function(session)
            products = list(query)
            self.assertEqual(1, len(products))

    def test_test_1(self):
        def function(session: DocumentSession):
            return (
                session.advanced.document_query_from_index_type(TestIndex, Product)
                .where_equals("name", "Bar")
                .intersect()
                .search("description", "Hello")
            )

        self.do_test(function)

    def test_test_2(self):
        def function(session: DocumentSession):
            return (
                session.advanced.document_query_from_index_type(TestIndex, Product)
                .search("description", "Hello")
                .intersect()
                .where_equals("name", "Bar")
            )

        self.do_test(function)
