from pyravendb.commands.raven_commands import PutDocumentCommand
from pyravendb.tests.test_base import TestBase
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


class Box(object):
    def __init__(self, items):
        self.items = items


class Item(object):
    def __init__(self, name):
        self.name = name


class TestLoad(TestBase):
    def setUp(self):
        super(TestLoad, self).setUp()

        with self.store.open_session() as session:
            session.store(Product("products/101", "test"))
            session.store(Product("products/10", "test"))
            session.store(Order("orders/105", "testing_order", 92, "products/101"))
            session.store(Company("company/1", "test", Product(name="testing_nested")))
            session.save_changes()

    def tearDown(self):
        super(TestLoad, self).tearDown()
        self.delete_all_topology_files()

    def test_can_handle_nested_values(self):
        request_executor = self.store.get_request_executor()
        put_command = PutDocumentCommand(key="testing/1", document={"items": [{"name": "oren"}]})
        request_executor.execute(put_command)
        with self.store.open_session() as session:
            i = session.load("testing/1", object_type=Box, nested_object_types={"items": Item})
            self.assertEqual("oren", i.items[0].name)

    def test_load_success(self):
        with self.store.open_session() as session:
            product = session.load("products/101")
            self.assertEqual(product.name, "test")

    def test_load_missing_document(self):
        with self.store.open_session() as session:
            self.assertEqual(session.load("products/0"), None)

    def test_multi_load(self):
        with self.store.open_session() as session:
            products = session.load(["products/101", "products/10"])
            self.assertEqual(len(products), 2)

    def test_multi_load_with_duplicate_id(self):
        with self.store.open_session() as session:
            products = session.load(["products/101", "products/101", "products/10"])
            self.assertEqual(len(products), 3)
            for product in products:
                self.assertIsNotNone(product)

    def test_load_track_entity(self):
        with self.store.open_session() as session:
            product = session.load("products/101")
            self.assertTrue(isinstance(product, Product))

    def test_load_track_entity_with_object_type(self):
        with self.store.open_session() as session:
            product = session.load("products/101", object_type=Product)
            self.assertTrue(isinstance(product, Product))

    def test_load_track_entity_with_object_type_and_nested_object(self):
        with self.store.open_session() as session:
            company = session.load("company/1", object_type=Company, nested_object_types={"product": Product})
            self.assertTrue(isinstance(company, Company) and isinstance(company.product, Product))

    def test_load_track_entity_with_object_type_fail(self):
        with self.store.open_session() as session:
            with self.assertRaises(exceptions.InvalidOperationException):
                session.load("products/101", object_type=Foo)

    def test_load_with_include(self):
        with self.store.open_session() as session:
            session.load("orders/105", includes="product_id")
            session.load("products/101")
        self.assertEqual(session.number_of_requests_in_session, 1)


if __name__ == "__main__":
    unittest.main()
