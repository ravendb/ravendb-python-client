from tests.test_base import TestBase
from store.document_store import DocumentStore
from custom_exceptions import exceptions
import unittest


class Product(object):
    def __init__(self, name):
        self.name = name


class Foo(object):
    def __init__(self, name, key):
        self.name = name
        self.key = key


class TestLoad(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestLoad, cls).setUpClass()
        cls.db.put("products/101", {"name": "test"}, {"Raven-Python-Type": "load_test.Product"})
        cls.db.put("products/10", {"name": "test"}, {})
        cls.document_store = DocumentStore(cls.default_url, cls.default_database)
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

    def test_load_track_entity(self):
        with self.document_store.open_session() as session:
            product = session.load("products/101")
            self.assertTrue(isinstance(product, Product))

    def test_load_track_entity_with_object_type(self):
        with self.document_store.open_session() as session:
            product = session.load("products/101", Product)
            self.assertTrue(isinstance(product, Product))

    def test_load_track_entity_with_object_type_fail(self):
        with self.document_store.open_session() as session:
            with self.assertRaises(exceptions.InvalidOperationException):
                session.load("products/101", Foo)

if __name__ == "__main__":
    unittest.main
