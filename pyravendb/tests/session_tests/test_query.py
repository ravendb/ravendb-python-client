from pyravendb.tests.test_base import TestBase
from pyravendb.raven_operations.maintenance_operations import PutIndexesOperation
from pyravendb.custom_exceptions import exceptions
from pyravendb.data.indexes import IndexDefinition, SortOptions, IndexFieldOptions
import unittest


class User:
    def __init__(self, name, age):
        self.name = name
        self.age = age


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
    def setUp(self):
        super(TestQuery, self).setUp()
        index_map = ("from doc in docs "
                     "select new {"
                     "name = doc.name,"
                     "key = doc.key,"
                     "doc_id = doc.key_doc.name}")
        index_definition = IndexDefinition(name="Testing_Sort", maps=index_map,
                                           fields={"key": IndexFieldOptions(sort_options=SortOptions.numeric),
                                                   "doc_id": IndexFieldOptions(storage=True)})

        maps = ("from order in docs.Orders "
                "select new {"
                "key = order.key,"
                "order_and_id = order.name+\"_\"+order.product_id}")
        second_index_definition = IndexDefinition(name="SelectTestingIndex", maps=maps,
                                                  fields={"order_and_id": IndexFieldOptions(storage=True)})

        self.store.maintenance.send(PutIndexesOperation(index_definition, second_index_definition))

        with self.store.open_session() as session:
            session.store(Product("test101", 2, "a"), "products/101")
            session.store(Product("test10", 3, "b"), "products/10")
            session.store(Product("test106", 4, "c"), "products/106")
            session.store(Product("test107", 5, None), "products/107")
            session.store(Product("test107", 6, None), "products/103")
            session.store(Product("new_testing", 90, "d"), "products/108")
            session.store(Order("testing_order", 92, "products/108"), "orders/105")
            session.store(Company("withNesting", Product(name="testing_order", key=4, order=None)), "company/1")
            session.save_changes()

    def tearDown(self):
        super(TestQuery, self).tearDown()
        self.delete_all_topology_files()

    def test_where_equal_dynamic_index(self):
        with self.store.open_session() as session:
            query_results = list(session.query(collection_name="Products").where_equals("name", "test101"))
            self.assertEqual(query_results[0].name, "test101")

    def test_can_use_exists_field(self):
        with self.store.open_session() as session:
            list(session.query(collection_name="Products").where_exists("name"))

    def test_where_not_equal(self):
        with self.store.open_session() as session:
            query_results = list(session.query(collection_name="Products").where_not_equals("name", "test101"))
            for result in query_results:
                assert result.name != "test101"

    def test_where_equal_double(self):
        with self.store.open_session() as session:
            query_results = list(session.query().where_equals("name", "test101").where_equals("key", 4))
            self.assertEqual(len(query_results), 2)

    def test_where_equal_double_and_operator(self):
        with self.store.open_session() as session:
            query_results = list(
                session.query(object_type=Product).where_equals(
                    "name", "test107").and_also().where_equals("key", 5))
            self.assertEqual(len(query_results), 1)

    def test_where_in(self):
        with self.store.open_session() as session:
            names = ["test101", "test107", "test106"]
            query_result = list(session.query().where_in("name", names))
            self.assertEqual(len(query_result), 4)
            for entity in query_result:
                assert entity.name in names

    def test_where_starts_with(self):
        with self.store.open_session() as session:
            query_result = list(session.query(Product).where_starts_with("name", "n"))
            self.assertEqual(query_result[0].name, "new_testing")

    def test_where_ends_with(self):
        with self.store.open_session() as session:
            query_result = list(session.query(Product).where_ends_with("name", "7"))
            self.assertEqual(query_result[0].name, "test107")

    def test_query_return_empty_query_with_not_exist_index(self):
        with self.store.open_session() as session:
            self.assertEqual(len(list(session.query(index_name="s").where(Tag="Products"))), 0)

    def test_query_success_with_where(self):
        with self.store.open_session() as session:
            query_results = list(session.query().where(name="test101", key=[4, 6, 90]))
            self.assertGreater(len(query_results), 0)

    def test_query_success_with_index(self):
        with self.store.open_session() as session:
            keys = [4, 6, 90]
            query_results = list(
                session.query(index_name="Testing_Sort", wait_for_non_stale_results=True).where(key=keys))
            self.assertEqual(len(query_results), 3)
            for result in query_results:
                assert result.key in keys

    def test_where_between(self):
        with self.store.open_session() as session:
            query_results = list(
                session.query(index_name="Testing_Sort", wait_for_non_stale_results=True).where_between("key", 2, 4))
            for result in query_results:
                self.assertGreaterEqual(result.key, 2)
                self.assertLessEqual(result.key, 4)

    def test_query_with_order_by(self):
        with self.store.open_session() as session:
            query_results = list(
                session.query(wait_for_non_stale_results=True, collection_name="products").where_not_none(
                    "order").order_by("order"))
            self.assertEqual(query_results[0].order, "a")

    def test_query_with_order_by_descending(self):
        with self.store.open_session() as session:
            query_results = list(
                session.query(wait_for_non_stale_results=True, ).where_not_none("order").order_by_descending("order"))
            self.assertEqual(query_results[0].order, "d")

    def test_where_not_None(self):
        found_none = False
        with self.store.open_session() as session:
            query_results = list(
                session.query(wait_for_non_stale_results=True, collection_name="products").where_not_none("order"))
            for result in query_results:
                if result.order is None:
                    found_none = True
                    break
            self.assertFalse(found_none)

    def test_where_with_include(self):
        with self.store.open_session() as session:
            list(session.query(wait_for_non_stale_results=True).where(key=92).include("product_id"))
            session.load("products/108")
        self.assertEqual(session.number_of_requests_in_session, 1)

    def test_query_with_nested_object(self):
        with self.store.open_session() as session:
            query_results = list(
                session.query(object_type=Company, nested_object_types={"product": Product}).where_equals(
                    "name", "withNesting"))
            self.assertTrue(isinstance(query_results[0], Company) and isinstance(query_results[0].product, Product))

    def test_query_with_raw_query(self):
        with self.store.open_session() as session:
            query_results = list(
                session.query(object_type=Company, nested_object_types={"product": Product}).raw_query(
                    "FROM Companies WHERE name='withNesting'"))
            self.assertTrue(isinstance(query_results[0], Company) and isinstance(query_results[0].product, Product))

    def test_raw_query_with_query_parameters(self):
        with self.store.open_session() as session:
            query_results = list(
                session.query(object_type=Company, nested_object_types={"product": Product}).raw_query(
                    "FROM Companies WHERE name= $p0", query_parameters={"p0": "withNesting"}))
            self.assertTrue(isinstance(query_results[0], Company) and isinstance(query_results[0].product, Product))

    def test_fail_after_raw_query_add(self):
        try:
            with self.store.open_session() as session:
                query_results = list(
                    session.query(object_type=Company, nested_object_types={"product": Product}).raw_query(
                        "FROM Companies WHERE name='withNesting'").where_starts_with("product", 'p'))
                self.assertTrue(isinstance(query_results[0], Company) and isinstance(query_results[0].product, Product))
        except exceptions.InvalidOperationException as e:
            self.assertTrue("raw_query was called" in str(e))

    def test_query_with_select(self):
        with self.store.open_session() as session:
            query_results = list(
                session.query(object_type=dict, wait_for_non_stale_results=True, index_name="SelectTestingIndex").where(
                    key=92).select("order_and_id"))
            for result in query_results:
                self.assertEqual(len(result), 1)
                self.assertTrue("order_and_id" in result)


if __name__ == "__main__":
    unittest.main()
