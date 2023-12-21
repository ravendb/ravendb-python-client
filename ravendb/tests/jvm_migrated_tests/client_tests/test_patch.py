from ravendb import PatchByQueryOperation, PatchOperation, PatchRequest, PatchStatus
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestPatch(TestBase):
    def setUp(self):
        super(TestPatch, self).setUp()

    def test_can_patch_many_documents(self):
        with self.store.open_session() as session:
            user = User(name="RavenDB")
            session.store(user, "users/1")
            session.save_changes()

            self.assertEqual(1, session.query(object_type=User).count_lazily().value)

        operation = PatchByQueryOperation('from Users update { this.name= "Patched" }')
        op = self.store.operations.send_async(operation)

        op.wait_for_completion()

        with self.store.open_session() as session:
            loaded_user = session.load("users/1", User)
            self.assertEqual("Patched", loaded_user.name)

    def test_can_patch_single_document(self):
        with self.store.open_session() as session:
            user = User(name="RavenDB")
            session.store(user, "users/1")
            session.save_changes()

        patch_operation = PatchOperation("users/1", None, PatchRequest.for_script('this.name = "Patched"'))
        status = self.store.operations.send(patch_operation).status
        self.assertEqual(PatchStatus.PATCHED, status)

        with self.store.open_session() as session:
            loaded_user = session.load("users/1", User)
            self.assertEqual("Patched", loaded_user.name)
