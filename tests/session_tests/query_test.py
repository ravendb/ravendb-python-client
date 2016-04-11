from tests.test_base import TestBase
from store.document_store import documentstore
from store.session_query import QueryOperator
from custom_exceptions import exceptions
from data.indexes import IndexDefinition, SortOptions


class Product(object):
    def __init__(self, name, key):
        self.name = name
        self.key = key


class TestQuery(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestQuery, cls).setUpClass()
        cls.index_map = ("from doc in docs "
                         "select new {"
                         "name = doc.name,"
                         "key = doc.key}")
        cls.index_sort = IndexDefinition(index_map=cls.index_map, sort_options={"key": SortOptions.float})
        cls.db.put_index("Testing_Sort", index_def=cls.index_sort, overwrite=True)
        cls.db.put("products/101", {"name": "test101", "key": 2, "order": "a"},
                   {"Raven-Entity-Name": "Products", "Raven-Python-Type": "query_test.Product"})
        cls.db.put("products/10", {"name": "test10", "key": 3, "order": "b"}, {})
        cls.db.put("products/106", {"name": "test106", "key": 4, "order": "c"}, {})
        cls.db.put("products/107", {"name": "test107", "key": 5, "order": None},
                   {"Raven-Entity-Name": "Products", "Raven-Python-Type": "query_test.Product"})
        cls.db.put("products/103", {"name": "test107", "key": 6},
                   {"Raven-Entity-Name": "Products", "Raven-Python-Type": "query_test.Product"})
        cls.db.put("products/108", {"name": "new_testing", "key": 90},
                   {"Raven-Entity-Name": "Products", "Raven-Python-Type": "query_test.Product"})
        cls.db.put("orders/105", {"name": "testing_order", "key": 92, "product": "products/108"},
                   {"Raven-Entity-Name": "Orders"})
        cls.document_store = documentstore(cls.default_url, cls.default_database)
        cls.document_store.initialize()

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
            self.assertEqual(query_results[0].order, "c")

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
            list(session.query(wait_for_non_stale_results=True, includes="product").where(key=92))
            session.load("products/108")
        self.assertEqual(session.number_of_requests_in_session, 1)
