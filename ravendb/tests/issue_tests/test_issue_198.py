from ravendb.tests.test_base import TestBase


class Product:
    def __init__(self, name: str = None):
        self.name = name
        self.Id = ""


class TestIssue198(TestBase):
    def setUp(self):
        super().setUp()

    def test_create_document_with_server_generated_guid(self):
        guid_from_server = None
        with self.store.open_session() as session:
            product = Product("Test")
            session.store(product)
            session.save_changes()
            guid_from_server = product.Id

        with self.store.open_session() as session:
            product = session.load(guid_from_server)
            self.assertEqual("Test", product.name)

    def test_can_create_multiple_documents_with_server_generated_guid(self):
        with self.store.open_session() as session:
            product1 = Product("Test1")
            product2 = Product("Test2")
            product3 = Product("Test3")
            session.store(product1)
            session.store(product2)
            session.store(product3)
            session.save_changes()
            guid_from_server1 = product1.Id
            guid_from_server2 = product2.Id
            guid_from_server3 = product3.Id

        with self.store.open_session() as session:
            product1 = session.load(guid_from_server1)
            self.assertEqual("Test1", product1.name)

            product2 = session.load(guid_from_server2)
            self.assertEqual("Test2", product2.name)

            product3 = session.load(guid_from_server3)
            self.assertEqual("Test3", product3.name)
