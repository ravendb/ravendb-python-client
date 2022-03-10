from ravendb.documents.commands.crud import GetDocumentsCommand
from ravendb.tests.test_base import TestBase, User


class Person(User):
    def __init__(self, name, age, address):
        super().__init__(name, age)
        self.address = address


class TestBasicDocument(TestBase):
    def setUp(self):
        super(TestBasicDocument, self).setUp()

    def test_get(self):
        dummy = {"name": None, "age": None, "Id": None}
        with self.store.open_session() as session:
            user1 = User("jacus", None)
            user2 = User("arek", None)
            session.store(user1, "users/1")
            session.store(user2, "users/2")
            session.save_changes()

        req_executor = self.store.get_request_executor()
        get_document_command = GetDocumentsCommand(
            GetDocumentsCommand.GetDocumentsByIdsCommandOptions(["users/1", "users/2"])
        )
        req_executor.execute_command(get_document_command)
        results = get_document_command.result.results
        self.assertEqual(len(results), 2)
        doc1 = results[0]
        doc2 = results[1]

        self.assertIsNotNone(doc1)
        self.assertIsNotNone(doc2)
        self.assertTrue("@metadata" in doc1.keys())
        self.assertTrue("@metadata" in doc2.keys())
        self.assertEqual(len(doc1), len(dummy) + 1)
        self.assertEqual(len(doc2), len(dummy) + 1)

        with self.store.open_session() as session:
            user1 = session.load("users/1", User)
            user2 = session.load("users/2", User)
            self.assertEqual(user1.name, "jacus")
            self.assertEqual(user2.name, "arek")

        get_document_command = GetDocumentsCommand(
            GetDocumentsCommand.GetDocumentsByIdsCommandOptions(["users/1", "users/2"], None, True)
        )
        req_executor.execute_command(get_document_command)
        results = get_document_command.result.results
        self.assertEqual(len(results), 2)
        doc1 = results[0]
        doc2 = results[1]

        self.assertIsNotNone(doc1)
        self.assertIsNotNone(doc2)
        self.assertTrue("@metadata" in doc1.keys())
        self.assertTrue("@metadata" in doc2.keys())
        self.assertEqual(len(doc1), 1)
        self.assertEqual(len(doc2), 1)

    def test_can_change_document_collection_with_delete_and_save(self):
        document_id = "users/1"
        with self.store.open_session() as session:
            user = User("John", 22)
            session.store(user, document_id)
            session.save_changes()

        with self.store.open_session() as session:
            session.delete(document_id)
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load(document_id, User)
            self.assertIsNone(user)

        with self.store.open_session() as session:
            person = Person("John", 22, None)
            session.store(person, document_id)
            session.save_changes()
