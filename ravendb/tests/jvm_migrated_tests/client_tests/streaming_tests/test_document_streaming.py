from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestDocumentStreaming(TestBase):
    def setUp(self):
        super(TestDocumentStreaming, self).setUp()

    def test_can_stream_documents_starting_with(self):
        with self.store.open_session() as session:
            for i in range(200):
                session.store(User())

            session.save_changes()

        count = 0

        with self.store.open_session() as session:
            query = "users/"
            stream = session.advanced.stream_starting_with(query, object_type=User)
            for result in stream:
                count += 1
                user = result.document
                self.assertIsNotNone(user)

        self.assertEqual(200, count)

    def test_can_stream_without_iteration_doesnt_leak_connection(self):
        with self.store.open_session() as session:
            for i in range(200):
                session.store(User())

            session.save_changes()

        for i in range(5):
            with self.store.open_session() as session:
                query = "users/"
                stream = session.advanced.stream_starting_with(query, object_type=User)
                for _ in stream:
                    pass
