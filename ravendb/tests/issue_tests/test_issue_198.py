from ravendb.infrastructure.orders import Company
from ravendb.primitives import constants
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
            metadata_id = session.advanced.get_metadata_for(product)["@id"]
            identity = session.advanced.get_document_id(product)
            id_prop = product.Id

            self.assertEqual(id_prop, identity)
            self.assertEqual(id_prop, metadata_id)
            self.assertEqual(id_prop, guid_from_server)

        with self.store.open_session() as session:
            product = session.load(guid_from_server)
            product.Id = ""
            session.save_changes()

        with self.store.open_session() as session:
            product = session.load(guid_from_server)
            metadata_id1 = session.advanced.get_metadata_for(product)[constants.Documents.Metadata.ID]
            identity1 = session.advanced.get_document_id(product)
            id_prop1 = product.Id

            self.assertEqual(id_prop, id_prop1)
            self.assertEqual(id_prop, metadata_id1)
            self.assertEqual(id_prop, identity1)

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

    def test_preserve_id_field(self):
        with self.store.open_session() as session:
            company = Company(Id="companies/222", name="Apple Inc.")
            session.store(company)
            session.save_changes()

        with self.store.open_session() as session:
            company = session.load("companies/222", Company)
            self.assertEqual("companies/222", company.Id)
            self.assertEqual("Apple Inc.", company.name)

    def test_docs_loaded_as_dictionary_dont_have_id_entry_and_its_id_is_available_via_metadata(self):
        with self.store.open_session() as session:
            company = Company(Id="companies/1", name="A Inc.")
            company_dict = {"Id": "companies/dict", "name": "Dict Inc."}
            session.store(company)
            session.store(company_dict)
            session.save_changes()

        with self.store.open_session() as session:
            company = session.load("companies/1", Company)
            company_dict = session.load("companies/dict", dict)

            self.assertNotIn("Id", company_dict)
            self.assertEqual("companies/dict", session.advanced.get_document_id(company_dict))
            self.assertEqual("companies/dict", session.advanced.get_metadata_for(company_dict)["@id"])

            self.assertEqual("companies/1", company.Id)
            self.assertEqual("companies/1", session.advanced.get_document_id(company))
            self.assertEqual("companies/1", session.advanced.get_metadata_for(company)["@id"])

        with self.store.open_session() as session:
            company_dict_as_company_object = session.load("companies/dict", Company)
            self.assertEqual("companies/dict", company_dict_as_company_object.Id)
            self.assertEqual("companies/dict", session.advanced.get_document_id(company_dict_as_company_object))
            self.assertEqual("companies/dict", session.advanced.get_metadata_for(company_dict_as_company_object)["@id"])
