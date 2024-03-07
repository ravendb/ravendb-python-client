import json

from ravendb import DocumentStore
from ravendb.documents.commands.results import GetDocumentsResult
from ravendb.infrastructure.orders import Employee
from ravendb.tests.test_base import TestBase


class TestLoadIntoStream(TestBase):
    def setUp(self):
        super().setUp()

    @staticmethod
    def insert_data(store: DocumentStore):
        with store.open_session() as session:

            def _insert_employee(name: str = None):
                employee = Employee(first_name=name)
                session.store(employee)

            _insert_employee("Aviv")
            _insert_employee("Iftah")
            _insert_employee("Tal")
            _insert_employee("Maxim")
            _insert_employee("Karmel")
            _insert_employee("Grisha")
            _insert_employee("Michael")
            session.save_changes()

    def test_can_load_starting_with_into_stream(self):
        self.insert_data(self.store)
        with self.store.open_session() as session:
            stream = session.advanced.load_starting_with_into_stream("employees/")
            json_node = json.loads(stream.decode("utf-8"))
            result = GetDocumentsResult.from_json(json_node)
            self.assertEqual(7, len(result.results))
            names = ["Aviv", "Iftah", "Tal", "Maxim", "Karmel", "Grisha", "Michael"]
            for name_from_results in [result["first_name"] for result in result.results]:
                self.assertIn(name_from_results, names)
                names.remove(name_from_results)
