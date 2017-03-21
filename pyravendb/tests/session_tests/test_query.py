from pyravendb.tests.test_base import TestBase
from pyravendb.store.document_store import DocumentStore
from pyravendb.custom_exceptions import exceptions
from pyravendb.data.indexes import IndexDefinition, SortOptions, QueryOperator, IndexFieldOptions
from pyravendb.d_commands.raven_commands import PutIndexesCommand
import unittest


class Product(object):
    def __init__(self, name, key, order):
        self.name = name
        self.key = key
        self.order = order


class Company(object):
    def __init__(self, name, product):
        self.name = name
        self.product = product


class Order(object):
    def __init__(self, name="", key=None, product_id=None):
        self.name = name
        self.key = key
        self.product_id = product_id


class TestQuery(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestQuery, cls).setUpClass()
        cls.index_map = ("from doc in docs "
                         "select new {"
                         "name = doc.name,"
                         "key = doc.key,"
                         "doc_id = doc.key+\"_\"+doc.name}")
        cls.index_sort = IndexDefinition(name="Testing_Sort", index_map=cls.index_map,
                                         fields={"key": IndexFieldOptions(sort_options=SortOptions.numeric),
                                                 "doc_id": IndexFieldOptions(storage=True)})

        cls.document_store = DocumentStore(cls.default_url, cls.default_database)
        cls.document_store.initialize()
        requests_executor = cls.document_store.get_request_executor()

        requests_executor.execute(PutIndexesCommand(cls.index_sort))

        with cls.document_store.open_session() as session:
            session.store(Product("test101", 2, "a"), "products/101")
            session.store(Product("test10", 3, "b"), "products/10")
            session.store(Product("test106", 4, "c"), "products/106")
            session.store(Product("test107", 5, None), "products/107")
            session.store(Product("test107", 6, None), "products/103")
            session.store(Product("new_testing", 90, "d"), "products/108")
            session.store(Order("testing_order", 92, "products/108"), "orders/105")
            session.store(Company("withNesting", Product(name="testing_order", key=4, order=None)), "company/1")
            session.save_changes()

    def test_where_equal_dynamic_index(self):
        with self.document_store.open_session() as session:
            query_results = list(session.query().where_equals("name", "test101"))
            self.assertEqual(query_results[0].name, "test101")

    def test_where_equal_double(self):
        with self.document_store.open_session() as session:
            query_results = list(session.query().where_equals("name", "test101").where_equals("key", 4))
            self.assertEqual(len(query_results), 2)

    def test_where_equal_double_and_operator(self):
        with self.document_store.open_session() as session:
            query_results = list(
                session.query(object_type=Product, using_default_operator=QueryOperator.AND).where_equals(
                    "name", "test107").where_equals("key", 5))
            self.assertEqual(len(query_results), 1)

    def test_where_in(self):
        with self.document_store.open_session() as session:
            query_result = list(session.query().where_in("name", ["test101", "test107", "test106"]))
            self.assertEqual(len(query_result), 4)

    def test_where_starts_with(self):
        with self.document_store.open_session() as session:
            query_result = list(session.query(Product).where_starts_with("name", "n"))
            self.assertEqual(query_result[0].name, "new_testing")

    def test_where_ends_with(self):
        with self.document_store.open_session() as session:
            query_result = list(session.query(Product).where_ends_with("name", "7"))
            self.assertEqual(query_result[0].name, "test107")

    def test_query_fail_with_index(self):
        with self.document_store.open_session() as session:
            with self.assertRaises(exceptions.ErrorResponseException):
                list(session.query(index_name="s").where(Tag="Products"))

    def test_query_success_with_where(self):
        with self.document_store.open_session() as session:
            query_results = list(session.query().where(name="test101", key=[4, 6, 90]))
            self.assertIsNotNone(query_results)

    def test_query_success_with_index(self):
        with self.document_store.open_session() as session:
            query_results = list(
                session.query(index_name="Testing_Sort", wait_for_non_stale_results=True).where(key=[4, 6, 90]))
            self.assertEqual(len(query_results), 3)

    def test_where_between(self):
        with self.document_store.open_session() as session:
            query_results = list(
                session.query(index_name="Testing_Sort", wait_for_non_stale_results=True).where_between("key", 2, 4))
            self.assertEqual(len(query_results), 1)

    def test_where_between_or_equal(self):
        with self.document_store.open_session() as session:
            query_results = list(
                session.query(index_name="Testing_Sort", wait_for_non_stale_results=True).where_between_or_equal("key",
                                                                                                                 2, 4))
            self.assertEqual(len(query_results), 3)

    def test_query_with_order_by(self):
        with self.document_store.open_session() as session:
            query_results = list(
                session.query(wait_for_non_stale_results=True).where_not_none("order").order_by("order"))
            self.assertEqual(query_results[0].order, "a")

    def test_query_with_order_by_descending(self):
        with self.document_store.open_session() as session:
            query_results = list(
                session.query(wait_for_non_stale_results=True).where_not_none("order").order_by_descending("order"))
            self.assertEqual(query_results[0].order, "d")

    def test_where_not_None(self):
        found_none = False
        with self.document_store.open_session() as session:
            query_results = list(session.query(wait_for_non_stale_results=True).where_not_none("order"))
            for result in query_results:
                if result.order is None:
                    found_none = True
                    break
            self.assertFalse(found_none)

    def test_where_with_include(self):
        with self.document_store.open_session() as session:
            list(session.query(wait_for_non_stale_results=True, includes="product_id").where(key=92))
            session.load("products/108")
        self.assertEqual(session.number_of_requests_in_session, 1)

    def test_query_with_nested_object(self):
        with self.document_store.open_session() as session:
            query_results = list(
                session.query(object_type=Company, nested_object_types={"product": Product}).where_equals(
                    "name", "withNesting"))
            self.assertTrue(isinstance(query_results[0], Company) and isinstance(query_results[0].product, Product))

    def test_query_with_fetch_terms(self):
        with self.document_store.open_session() as session:
            query_results = list(
                session.query(index_name="Testing_Sort", wait_for_non_stale_results=True).where_between_or_equal
                ("key", 2, 4).select("doc_id"))

            found_in_all = True
            for result in query_results:
                if not hasattr(result, "doc_id"):
                    found_in_all = False
                    break

            self.assertTrue(found_in_all)


if __name__ == "__main__":
    unittest.main()
