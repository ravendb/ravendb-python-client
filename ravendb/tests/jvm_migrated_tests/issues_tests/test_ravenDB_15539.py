from ravendb.documents.conventions.misc import ShouldIgnoreEntityChanges
from ravendb.documents.store.definition import DocumentStore
from ravendb.tests.test_base import TestBase


class User:
    def __init__(self, name: str, ignore_changes: bool = False):
        self.name = name
        self.ignore_changes = ignore_changes


class TestRavenDB15539(TestBase):
    def setUp(self):
        super(TestRavenDB15539, self).setUp()

    def _customize_store(self, store: DocumentStore) -> None:
        class Ignore(ShouldIgnoreEntityChanges):
            def check(
                self,
                session_operations: "InMemoryDocumentSessionOperations",
                entity: object,
                document_id: str,
            ) -> bool:
                return isinstance(entity, User) and entity.ignore_changes

        store.conventions.should_ignore_entity_changes = Ignore()

    def test_can_ignore_changes(self):
        with self.store.open_session() as session:
            user = User("Oren")
            session.store(user, "users/oren")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/oren", User)
            user.name = "Arava"
            user.ignore_changes = True
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/oren", User)
            self.assertEqual("Oren", user.name)
