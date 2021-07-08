from pyravendb.tests.test_base import TestBase, User
from pyravendb.commands.raven_commands import GetDocumentCommand


class TestBasicDocument(TestBase):
    def setUp(self):
        super(TestBasicDocument, self).setUp()

    def test_get(self):
        dummy = {'name': None, 'age': None}
        with self.store.open_session() as session:
            user1 = User("jacus", None)
            user2 = User("arek", None)
            session.store(user1, "users/1")
            session.store(user2, "users/2")
            session.save_changes()

        req_executor = self.store.get_request_executor()
        get_document_command = GetDocumentCommand(["users/1","users/2"])
        results = req_executor.execute(get_document_command)['Results']
        self.assertEqual(len(results), 2)
        doc1 = results[0]
        doc2 = results[1]

        self.assertIsNotNone(doc1)  # Should it be like this? Or should we dive into document fields?
        self.assertIsNotNone(doc2)
        self.assertTrue('@metadata' in doc1.keys())
        self.assertTrue('@metadata' in doc2.keys())
        self.assertEqual(len(doc1),len(dummy)+1)
        self.assertEqual(len(doc2),len(dummy)+1)

        with self.store.open_session() as session:
            user1 = session.load("users/1", User)
            user2 = session.load("users/2", User)
            self.assertEqual(user1.name, "jacus")
            self.assertEqual(user2.name, "arek")

        get_document_command = GetDocumentCommand(["users/1", "users/2"], None, True)
        results = req_executor.execute(get_document_command)['Results']
        self.assertEqual(len(results), 2)
        doc1 = results[0]
        doc2 = results[1]

        self.assertIsNotNone(doc1)
        self.assertIsNotNone(doc2)
        self.assertTrue('@metadata' in doc1.keys())
        self.assertTrue('@metadata' in doc2.keys())
        self.assertEqual(len(doc1), 1)
        self.assertEqual(len(doc2), 1)

