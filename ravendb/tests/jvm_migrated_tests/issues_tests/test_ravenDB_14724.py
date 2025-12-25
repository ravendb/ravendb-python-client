from ravendb.documents.operations.revisions import RevisionsConfiguration, ConfigureRevisionsOperation

from ravendb.tests.test_base import TestBase, User

from ravendb.primitives.constants import Documents


class TestRavenDB14724(TestBase):
    def setUp(self):
        super(TestRavenDB14724, self).setUp()

    def test_delete_document_and_revisions(self):
        user = User("Raven")
        id = "user/1"

        self.setup_revisions(self.store, True, 5)

        with self.store.open_session() as session:
            session.store(user, id)
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load(id, object_type=User)
            user.age = 10

            session.store(user)
            session.save_changes()

            metadata = session.advanced.get_metadata_for(user)

            self.assertEqual("HasRevisions", metadata[Documents.Metadata.FLAGS])

            revisions = session.advanced.revisions.get_for(id, object_type=User)
            self.assertEqual(2, len(revisions))

            session.delete(id)
            session.save_changes()

            configuration = RevisionsConfiguration()
            operation = ConfigureRevisionsOperation(configuration)
            self.store.maintenance.send(operation)

        with self.store.open_session() as session:
            session.store(user, id)
            session.save_changes()

            self.setup_revisions(self.store, True, 5)

        with self.store.open_session() as session:
            user = session.load(id, object_type=User)
            revisions = session.advanced.revisions.get_for(id, object_type=User)
            self.assertEqual(0, len(revisions))