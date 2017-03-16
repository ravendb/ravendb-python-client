from pyravendb.tests.test_base import TestBase
from pyravendb.store.document_store import DocumentStore
from pyravendb.d_commands.raven_commands import PutDocumentCommand
from pyravendb.custom_exceptions import exceptions
import unittest


class Product(object):
    def __init__(self, Id=None, name=""):
        self.Id = Id
        self.name = name


class Company(object):
    def __init__(self, Id=None, name="", product=None):
        self.Id = Id
        self.name = name
        self.product = product


class Foo(object):
    def __init__(self, name, key):
        self.name = name
        self.key = key


class Time(object):
    def __init__(self, td):
        self.td = td


class Order(object):
    def __init__(self, Id=None, name="", key=None, product_id=None):
        self.Id = Id
        self.name = name
        self.key = key
        self.product_id = product_id


class TestLoad(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestLoad, cls).setUpClass()

        cls.document_store = DocumentStore(cls.default_url, cls.default_database)
        cls.document_store.initialize()

        with cls.document_store.open_session() as session:
            session.store(Product("products/101", "test"))
            session.store(Product("products/10", "test"))
            session.store(Order("orders/105", "testing_order", 92, "products/101"))
            session.store(Company("company/1", "test", Product(name="testing_nested")))
            session.save_changes()

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
            session.load("orders/105", includes="product_id")
            session.load("products/101")
        self.assertEqual(session.number_of_requests_in_session, 1)


if __name__ == "__main__":
    unittest.main()
