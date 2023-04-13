from typing import Optional

from ravendb.documents.session.misc import SessionOptions
from ravendb.documents.session.query import QueryStatistics
from ravendb.infrastructure.orders import Product, Supplier
from ravendb.tests.test_base import TestBase


class TestRavenDB11217(TestBase):
    def setUp(self):
        super(TestRavenDB11217, self).setUp()

    def test_session_wide_no_tracking_should_work(self):
        with self.store.open_session() as session:
            supplier = Supplier()
            supplier.name = "Supplier1"

            session.store(supplier)

            product = Product()
            product.name = "Product1"
            product.supplier = supplier.Id

            session.store(product)
            session.save_changes()

        no_tracking_options = SessionOptions()
        no_tracking_options.no_tracking = True

        with self.store.open_session(session_options=no_tracking_options) as session:
            supplier = Supplier()
            supplier.name = "Supplier2"

            with self.assertRaises(RuntimeError):
                session.store(supplier)

        with self.store.open_session(session_options=no_tracking_options) as session:
            self.assertEqual(0, session.advanced.number_of_requests)

            product1 = session.load("products/1-A", Product, lambda b: b.include_documents("supplier"))

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertIsNotNone(product1)
            self.assertEqual("Product1", product1.name)
            self.assertFalse(session.advanced.is_loaded(product1.Id))
            self.assertFalse(session.advanced.is_loaded(product1.supplier))

            supplier = session.load(product1.supplier, Supplier)
            self.assertIsNotNone(supplier)
            self.assertEqual("Supplier1", supplier.name)
            self.assertEqual(2, session.advanced.number_of_requests)
            self.assertFalse(session.advanced.is_loaded(supplier.Id))

            product2 = session.load("products/1-A", Product, lambda b: b.include_documents(("supplier")))
            self.assertNotEqual(product2, product1)

        with self.store.open_session(session_options=no_tracking_options) as session:
            self.assertEqual(0, session.advanced.number_of_requests)

            product1 = session.advanced.load_starting_with("products/", Product)[0]

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertIsNotNone(product1)
            self.assertEqual("Product1", product1.name)
            self.assertFalse(session.advanced.is_loaded(product1.Id))
            self.assertFalse(session.advanced.is_loaded(product1.supplier))

            supplier = session.advanced.load_starting_with(product1.supplier, Supplier)[0]

            self.assertIsNotNone(supplier)
            self.assertEqual("Supplier1", supplier.name)
            self.assertEqual(2, session.advanced.number_of_requests)
            self.assertFalse(session.advanced.is_loaded(supplier.Id))

            product2 = session.advanced.load_starting_with("products/", Product)[0]
            self.assertNotEqual(product2, product1)

        with self.store.open_session(session_options=no_tracking_options) as session:
            self.assertEqual(0, session.advanced.number_of_requests)
            products = list(session.query(object_type=Product).include("supplier"))

            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertEqual(1, len(products))

            product1 = products[0]
            self.assertIsNotNone(product1)
            self.assertFalse(session.advanced.is_loaded(product1.Id))
            self.assertFalse(session.advanced.is_loaded(product1.supplier))

            supplier = session.load(product1.supplier, Supplier)
            self.assertIsNotNone(supplier)
            self.assertEqual("Supplier1", supplier.name)
            self.assertEqual(2, session.advanced.number_of_requests)
            self.assertFalse(session.advanced.is_loaded(supplier.Id))

            products = list(session.query(object_type=Product).include("supplier"))
            self.assertEqual(1, len(products))

            product2 = products[0]
            self.assertNotEqual(product2, product1)

        with self.store.open_session() as session:
            session.counters_for("products/1-A").increment("c1")
            session.save_changes()

        with self.store.open_session(session_options=no_tracking_options) as session:
            product1 = session.load("products/1-A", Product)
            counters = session.counters_for(product1.Id)

            counters.get("c1")

            self.assertEqual(2, session.advanced.number_of_requests)

            counters.get("c1")

            self.assertEqual(3, session.advanced.number_of_requests)

            val1 = counters.get_all()

            self.assertEqual(4, session.advanced.number_of_requests)

            val2 = counters.get_all()

            self.assertEqual(5, session.advanced.number_of_requests)

            self.assertNotEqual(id(val2), id(val1))


def test_session_wide_no_caching_should_work(self):
    with self.store.open_session() as session:
        stats: Optional[QueryStatistics] = None

        def __stats_callback(statistics: QueryStatistics):
            nonlocal stats
            stats = statistics

        list(session.query(object_type=Product).statistics(__stats_callback).where_equals("name", "HR"))
        self.assertGreaterEqual(stats.duration_in_ms, 0)
        list(session.query(object_type=Product).statistics(__stats_callback).where_equals("name", "HR"))
        self.assertEqual(-1, stats.duration_in_ms)

    no_cache_options = SessionOptions()
    no_cache_options.no_caching = True

    with self.store.open_session(session_options=no_cache_options) as session:
        list(session.query(object_type=Product).statistics(__stats_callback).where_equals("name", "HR"))
        self.assertGreaterEqual(stats.duration_in_ms, 0)
        list(session.query(object_type=Product).statistics(__stats_callback).where_equals("name", "HR"))
        self.assertGreaterEqual(stats.duration_in_ms, 0)
