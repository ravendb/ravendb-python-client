from pyravendb.tests.test_base import TestBase
from pyravendb.store.document_store import documentstore
from pyravendb.custom_exceptions import exceptions
import unittest


class Product(object):
    def __init__(self, name):
        self.name = name


class Company(object):
    def __init__(self, name, product):
        self.name = name
        self.product = product


class Foo(object):
    def __init__(self, name, key):
        self.name = name
        self.key = key


class Time(object):
    def __init__(self, td):
        self.td = td


class TestLoad(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestLoad, cls).setUpClass()
        cls.db.put("products/101", {"name": "test"},
                   {"Raven-Python-Type": Product.__module__+".Product"})
        cls.db.put("products/10", {"name": "test"}, {})
        cls.db.put("orders/105", {"name": "testing_order", "key": 92, "product": "products/101"},
                   {"Raven-Entity-Name": "Orders"})
        cls.db.put("company/1", {"name": "test", "product": {"name": "testing_nested"}}, {})
        cls.document_store = documentstore(cls.default_url, cls.default_database)
        cls.document_store.initialize()

    def test_load_success(self):
        with self.document_store.open_session() as session:
            product = session.load("products/101")
            self.assertEqual(product.name, "test")

    def test_load_missing_document(self):
        with self.document_store.open_session() as session:
            self.assertEqual(session.load("products/0"), None)

    def test_multi_load(self):
        with self.document_store.open_session() as session:
            products = session.load(["products/101", "products/10"])
            self.assertEqual(len(products), 2)

    def test_multi_load_with_duplicate_id(self):
        with self.document_store.open_session() as session:
            products = session.load(["products/101", "products/101", "products/10"])
            self.assertEqual(len(products), 3)
            for product in products:
                self.assertIsNotNone(product)

    def test_load_track_entity(self):
        with self.document_store.open_session() as session:
            product = session.load("products/101")
            self.assertTrue(isinstance(product, Product))

    def test_load_track_entity_with_object_type(self):
        with self.document_store.open_session() as session:
            product = session.load("products/101", object_type=Product)
            self.assertTrue(isinstance(product, Product))

    def test_load_track_entity_with_object_type_and_nested_object(self):
        with self.document_store.open_session() as session:
            company = session.load("company/1", object_type=Company, nested_object_types={"product": Product})
            self.assertTrue(isinstance(company, Company) and isinstance(company.product, Product))

    def test_load_track_entity_with_object_type_fail(self):
        with self.document_store.open_session() as session:
            with self.assertRaises(exceptions.InvalidOperationException):
                session.load("products/101", object_type=Foo)

    def test_load_with_include(self):
        with self.document_store.open_session() as session:
            session.load("orders/105", includes="product")
            session.load("products/101")
        self.assertEqual(session.number_of_requests_in_session, 1)


if __name__ == "__main__":
    unittest.main()
