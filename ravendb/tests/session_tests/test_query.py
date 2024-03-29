from datetime import datetime

from ravendb.documents.indexes.definitions import IndexDefinition
from ravendb.documents.operations.indexes import PutIndexesOperation
from ravendb.tests.test_base import TestBase
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
        index_map = "from doc in docs " "select new {" "name = doc.name," "key = doc.key," "doc_id = doc.key_doc.name}"
        index_definition = IndexDefinition()
        index_definition.name = "Testing_Sort"
        index_definition.maps = index_map
        # fields -> "key": IndexFieldOptions(),  sort options numeric

        maps = (
            "from order in docs.Orders "
            "select new {"
            "key = order.key,"
            'order_and_id = order.name+"_"+order.product_id}'
        )
        second_index_definition = IndexDefinition()
        second_index_definition.name = "SelectTestingIndex"
        second_index_definition.maps = maps
        # fields -> "order_and_id": IndexFieldOptions(),  sort options numeric

        self.store.maintenance.send(PutIndexesOperation(index_definition, second_index_definition))

        with self.store.open_session() as session:
            session.store(Product("test101", 2, "a"), "products/101")
            session.store(Product("test10", 3, "b"), "products/10")
            session.store(Product("test106", 4, "c"), "products/106")
            session.store(Product("test107", 5, None), "products/107")
            session.store(Product("test107", 6, None), "products/103")
            session.store(Product("new_testing", 90, "d"), "products/108")
            session.store(Order("testing_order", 92, "products/108"), "orders/105")
            session.store(
                Company("withNesting", Product(name="testing_order", key=4, order=None)),
                "company/1",
            )
            session.save_changes()

    def tearDown(self):
        super(TestQuery, self).tearDown()
        self.delete_all_topology_files()

    def test_where_equal_dynamic_index(self):
        with self.store.open_session() as session:
            query_results = list(session.query_collection("Products").where_equals("name", "test101"))
            self.assertEqual(query_results[0].name, "test101")

    def test_can_use_exists_field(self):
        with self.store.open_session() as session:
            list(session.query_collection("Products").where_exists("name"))

    def test_can_query_on_date_time(self):
        with self.store.open_session() as session:
            list(session.query_collection("Products").where_equals("at", datetime.now()))

    def test_can_query_on_date_time_range(self):
        with self.store.open_session() as session:
            list(session.query_collection("Products").where_between("at", datetime.now(), datetime.now()))

    def test_where_not_equal(self):
        with self.store.open_session() as session:
            query_results = list(session.query_collection("Products").where_not_equals("name", "test101"))
            for result in query_results:
                assert result.name != "test101"

    def test_where_equal_double(self):
        with self.store.open_session() as session:
            query_results = list(session.query().where_equals("name", "test101").or_else().where_equals("key", 4))
            self.assertEqual(len(query_results), 2)

    def test_where_equal_double_and_operator(self):
        with self.store.open_session() as session:
            query_results = list(
                session.query(object_type=Product).where_equals("name", "test107").and_also().where_equals("key", 5)
            )
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
            query_result = list(session.query(object_type=Product).where_starts_with("name", "n"))
            self.assertEqual(query_result[0].name, "new_testing")

    def test_where_ends_with(self):
        with self.store.open_session() as session:
            query_result = list(session.query(object_type=Product).where_ends_with("name", "7"))
            self.assertEqual(query_result[0].name, "test107")

    def test_query_success_with_where(self):
        with self.store.open_session() as session:
            query_results = list(session.query().where(name="test101", key=[4, 6, 90]))
            self.assertGreater(len(query_results), 0)

    def test_query_success_with_index(self):
        with self.store.open_session() as session:
            keys = [4, 6, 90]
            query_results = list(session.query_index("Testing_Sort").wait_for_non_stale_results().where_in("key", keys))
            self.assertEqual(len(query_results), 3)
            for result in query_results:
                assert result.key in keys

    def test_where_between(self):
        with self.store.open_session() as session:
            query_results = list(
                session.query_index("Testing_Sort").wait_for_non_stale_results().where_between("key", 2, 4)
            )
            for result in query_results:
                self.assertGreaterEqual(result.key, 2)
                self.assertLessEqual(result.key, 4)

    def test_query_with_order_by(self):
        with self.store.open_session() as session:
            query_results = list(
                session.query_collection("products")
                .wait_for_non_stale_results()
                .where_not_equals("order", None)
                .order_by("order")
            )
            self.assertEqual(query_results[0].order, "a")

    def test_query_with_order_by_descending(self):
        with self.store.open_session() as session:
            query_results = list(
                session.query(object_type=Product)
                .wait_for_non_stale_results()
                .where_not_equals("order", None)
                .order_by_descending("order")
            )
            self.assertEqual(query_results[0].order, "d")

    def test_where_not_None(self):
        found_none = False
        with self.store.open_session() as session:
            query_results = list(
                session.query_collection("products").wait_for_non_stale_results().where_not_none("order")
            )
            for result in query_results:
                if result.order is None:
                    found_none = True
                    break
            self.assertFalse(found_none)

    def test_where_with_include(self):
        with self.store.open_session() as session:
            list(session.query().wait_for_non_stale_results().where(key=92).include("product_id"))
            session.load("products/108")
        self.assertEqual(session.number_of_requests, 1)


if __name__ == "__main__":
    unittest.main()
