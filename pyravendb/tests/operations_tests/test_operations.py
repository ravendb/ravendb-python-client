from pyravendb.tests.test_base import *
from pyravendb.raven_operations.operations import *
import unittest


class TestOperations(TestBase):
    def setUp(self):
        super(TestOperations, self).setUp()
        with self.store.open_session() as session:
            user = User(name="Idan", age="29")
            session.store(user, key="users/1-A")
            session.store(Dog(name="Fazy", owner=user), key="dogs/1-A")
            session.save_changes()

    def tearDown(self):
        super(TestOperations, self).tearDown()
        TestBase.delete_all_topology_files()

    def test_put_attachment(self):
        binary_list = bin(int(''.join(map(str, [1, 2, 3, 4, 5, 70, 90]))) << 1)
        put_result = self.store.operations.send(
            PutAttachmentOperation("users/1-A", "my_picture", binary_list, content_type="image/png"))
        self.assertIsNotNone(put_result)
        self.assertEqual(put_result["ContentType"], "image/png")
        self.assertEqual(put_result["Name"], "my_picture")

    def test_delete_attachment(self):
        self.test_put_attachment()
        get_attachment_operation = GetAttachmentOperation("users/1-A", "my_picture", AttachmentType.document, None)
        attachment = self.store.operations.send(get_attachment_operation)
        self.assertIsNotNone(attachment)
        self.store.operations.send(DeleteAttachmentOperation("users/1-A", "my_picture"))
        attachment = self.store.operations.send(get_attachment_operation)
        self.assertIsNone(attachment)

    def test_patch_by_index(self):
        self.store.maintenance.send(PutIndexesOperation(
            IndexDefinition("Patches", maps="from doc in docs.Patches select new {patched = doc.patched}")))

        with self.store.open_session() as session:
            session.store(Patch(patched=False))
            session.store(Patch(patched=False))
            session.store(Patch(patched=True))
            session.save_changes()
            # doing the query here to make sure the query won't be stale
            query_result = list(
                session.query(wait_for_non_stale_results=True).raw_query("FROM INDEX 'Patches' Where patched=False"))
            assert query_result

        options = QueryOperationOptions(allow_stale=False, retrieve_details=True)
        operation = PatchByQueryOperation(
            query_to_update=IndexQuery(query="FROM INDEX 'Patches' Where patched=False UPDATE {{this.patched=true}}"),
            options=options)
        operation_id = self.store.operations.send(operation)
        result = self.store.operations.wait_for_operation_complete(operation_id=operation_id["operation_id"])
        self.assertTrue(len(result["Result"]["Details"]) == 2)

    def test_fail_patch_wrong_index_name(self):
        options = QueryOperationOptions(allow_stale=False, retrieve_details=True)
        operation = PatchByQueryOperation(
            query_to_update=IndexQuery(query="FROM INDEX 'None' UPDATE {{this.name='NotExist'}}",
                                       wait_for_non_stale_results=True), options=options)
        with self.assertRaises(exceptions.InvalidOperationException):
            result = self.store.operations.send(operation)
            if result:
                self.store.operations.wait_for_operation_complete(result['operation_id'])
            else:
                raise exceptions.ErrorResponseException("Got empty or None response from the server")

    def test_delete_by_index(self):
        self.store.maintenance.send(PutIndexesOperation(
            IndexDefinition("Users", maps="from doc in docs.Users select new {name=doc.name}")))

        with self.store.open_session() as session:
            session.store(User(name="delete", age=0), key="deletes/1-A")
            session.save_changes()
            # doing the query here to make sure the query won't be stale
            query_result = list(session.query(wait_for_non_stale_results=True).raw_query("FROM INDEX 'Users'"))
            assert query_result

        index_query = IndexQuery(query="FROM INDEX 'Users' WHERE name='delete'")
        operation_id = self.store.operations.send(
            DeleteByQueryOperation(index_query))
        result = self.store.operations.wait_for_operation_complete(operation_id=operation_id["operation_id"])
        self.assertEqual(result["Result"]["Total"], 1)

    def test_patch_by_collection(self):
        with self.store.open_session() as session:
            session.store(Patch(patched=False))
            session.store(Patch(patched=False))
            session.store(Patch(patched=False))
            session.save_changes()

        index_query = IndexQuery("From Patches Update {{this.patched=true}}", wait_for_non_stale_results=True)

        operation_id = self.store.operations.send(
            PatchByQueryOperation(query_to_update=index_query))
        result = self.store.operations.wait_for_operation_complete(operation_id=operation_id["operation_id"])
        self.assertTrue(result["Result"]["Total"] == 3)

    def test_delete_by_collection_(self):
        index_query = IndexQuery("From Users")
        operation_id = self.store.operations.send(DeleteByQueryOperation(query_to_delete=index_query))["operation_id"]
        result = self.store.operations.wait_for_operation_complete(operation_id)
        self.assertTrue(result["Result"]["Total"] == 1)


if __name__ == "__main__":
    unittest.main()
