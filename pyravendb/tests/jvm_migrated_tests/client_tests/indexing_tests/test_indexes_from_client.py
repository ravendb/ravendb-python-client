from pyravendb.documents.indexes.definitions import FieldIndexing, FieldStorage
from pyravendb.documents.indexes.index_creation import IndexCreation, AbstractIndexCreationTask
from pyravendb.infrastructure.entities import User
from pyravendb.tests.test_base import TestBase


class Users_ByName(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = "from u in docs.Users select new { u.name }"
        self._index("name", FieldIndexing.SEARCH)

        self._index_suggestions.add("name")

        self._store("name", FieldStorage.YES)


class TestIndexesFromClient(TestBase):
    def setUp(self):
        super(TestIndexesFromClient, self).setUp()

    def test_can_create_indexes_using_index_creation(self):
        IndexCreation.create_indexes([Users_ByName()], self.store)

        with self.store.open_session() as session:
            user1 = User("users/1", "Marcin")
            session.store(user1)
            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            users = list(session.query_index_type(Users_ByName, User))
            self.assertEqual(1, len(users))
