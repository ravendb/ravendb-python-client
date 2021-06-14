import unittest

from pyravendb.commands.raven_commands import GetDocumentCommand
from pyravendb.custom_exceptions.exceptions import InvalidOperationException
from pyravendb.data.document_conventions import DocumentConventions
from pyravendb.tests.test_base import TestBase


class Address:
    def __init__(self, street, city, state):
        self.street = street
        self.city = city
        self.state = state
        self.Id: str = None


class Customer:
    def __init__(self, name):
        self.name = name
        self.Id: str = None

class SomeClass:
    def __init__(self, name):
        self.name = name
        self.Id: str = None


class TesteCollectionName(TestBase):
    def setUp(self):
        self.setConvention(DocumentConventions(collection_names={Address: 'Address',SomeClass:'SomeNewCoolName'}))
        super(TesteCollectionName, self).setUp()
        self.requests_executor = self.store.get_request_executor()

    def tearDown(self):
        super(TesteCollectionName, self).tearDown()
        self.delete_all_topology_files()

    def test_id_prefix_default(self):
        with self.store.open_session() as session:
            c = Customer(name="John")
            session.store(c)
            session.save_changes()
            assert c.Id.startswith('customers/')

    def test_collection_name_default(self):
        with self.store.open_session() as session:
            c = Customer(name="John")
            session.store(c, key="customer/123123-ABC")
            session.save_changes()
            response = self.requests_executor.execute(GetDocumentCommand("customer/123123-ABC"))
            assert response["Results"][0]["@metadata"]["@collection"] == "Customers"

    def test_id_prefix_custom(self):
        with self.store.open_session() as session:
            c = Address(street='Baker Street', city='Westminster', state='London')
            session.store(c)
            session.save_changes()
            assert c.Id.startswith('address/')

    def test_collection_name_custom(self):
        with self.store.open_session() as session:
            c = Address(street='Baker Street', city='Westminster', state='London')
            session.store(c, key="address/123-A")
            session.save_changes()
            response = self.requests_executor.execute(GetDocumentCommand("address/123-A"))
            assert response["Results"][0]["@metadata"]["@collection"] == "Address"

    def test_id_prefix_custom_composite_name(self):
        with self.store.open_session() as session:
            c = SomeClass(name="xpto")
            session.store(c)
            session.save_changes()
            assert c.Id.startswith('SomeNewCoolName/')

    def test_collection_name_custom_composite_name(self):
        with self.store.open_session() as session:
            c = SomeClass(name="xpto")
            session.store(c,key="SomeNewCoolName/111-A")
            session.save_changes()
            response = self.requests_executor.execute(GetDocumentCommand("SomeNewCoolName/111-A"))
            assert response["Results"][0]["@metadata"]["@collection"] == "SomeNewCoolName"

    def test_shouldnt_allow_change_after_initialize(self):
        with self.assertRaises(InvalidOperationException) as context:
            self.store.conventions.default_collection_names = {Address: 'Addresses'}


if __name__ == "__main__":
    unittest.main()
