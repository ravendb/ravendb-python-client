from tests.test_base import TestBase
from store.document_store import DocumentStore
from custom_exceptions import exceptions


class TestDelete(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestDelete, cls).setUpClass()
        cls.db.put("products/101", {"name": "test"}, {"Raven-Python-Type": "delete_test.Product"})
        cls.db.put("products/10", {"name": "test"}, {})
        cls.db.put("products/106", {"name": "test"}, {})
        cls.db.put("products/107", {"name": "test"}, {})
        cls.document_store = DocumentStore(cls.default_url, cls.default_database)
        cls.document_store.initialize()

    def test_delete_with_key_with_save_session(self):
        with self.document_store.open_session() as session:
            session.delete("products/101")
            session.save_changes()
            self.assertIsNone(session.load("products/101"))

    def test_delete_with_key_without_save_session(self):
        with self.document_store.open_session() as session:
            session.delete("products/10")
            self.assertIsNone(session.load("products/101"))

    def test_delete_after_change_fail(self):
        with self.document_store.open_session() as session:
            product = session.load("products/106")
            product.name = "testing"
            with self.assertRaises(exceptions.InvalidOperationException):
                session.delete("products/106")

    def test_delete_after_change_success_with_save_session(self):
        with self.document_store.open_session() as session:
            product = session.load("products/107")
            product.name = "testing"
            session.delete_by_entity(product)
            session.save_changes()
            self.assertIsNone(session.load("products/107"))
