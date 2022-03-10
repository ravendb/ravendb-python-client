from ravendb.exceptions.exceptions import InvalidOperationException, ErrorResponseException
from ravendb.documents.indexes.definitions import IndexDefinition
from ravendb.documents.operations.attachments import (
    PutAttachmentOperation,
    DeleteAttachmentOperation,
)
from ravendb.documents.operations.indexes import PutIndexesOperation
from ravendb.documents.operations.misc import QueryOperationOptions, DeleteByQueryOperation
from ravendb.documents.operations.patch import PatchByQueryOperation
from ravendb.documents.queries.index_query import IndexQuery
from ravendb.tests.test_base import *
from ravendb.documents.operations.operation import Operation as NewOperation
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
        binary_list = bin(int("".join(map(str, [1, 2, 3, 4, 5, 70, 90]))) << 1)
        put_result = self.store.operations.send(
            PutAttachmentOperation("users/1-A", "my_picture", binary_list, content_type="image/png")
        )
        self.assertIsNotNone(put_result)
        self.assertEqual("users/1-A", put_result.document_id)

    def test_delete_attachment(self):
        self.test_put_attachment()
        with self.store.open_session() as session:
            user = session.load("users/1-A")
            metadata = session.get_metadata_for(user)
            attachments = metadata.metadata.get(constants.Documents.Metadata.ATTACHMENTS, None)
            self.assertTrue(attachments)  # not None, more than 0

        self.store.operations.send(DeleteAttachmentOperation("users/1-A", "my_picture"))
        with self.store.open_session() as session:
            user = session.load("users/1-A")
            metadata = session.get_metadata_for(user)
            attachments = metadata.metadata.get(constants.Documents.Metadata.ATTACHMENTS, None)
            self.assertFalse(attachments)  # 0 or None

    def test_patch_by_index(self):
        index = IndexDefinition()
        index.name = "Patches"
        index.maps = ["from doc in docs.Patches select new {patched = doc.patched}"]
        self.store.maintenance.send(PutIndexesOperation(index))

        with self.store.open_session() as session:
            session.store(Patch(patched=False))
            session.store(Patch(patched=False))
            session.store(Patch(patched=True))
            session.save_changes()
            # doing the query here to make sure the query won't be stale
            query_result = list(
                session.advanced.raw_query("FROM INDEX 'Patches' where patched=False").wait_for_non_stale_results()
            )
            assert query_result is not None

        options = QueryOperationOptions(allow_stale=False, retrieve_details=True)
        operation = PatchByQueryOperation(
            query_to_update=IndexQuery(query="FROM INDEX 'Patches' Where patched=False UPDATE {{this.patched=true}}"),
            options=options,
        )
        response = self.store.operations.send(operation)

        operation = NewOperation(
            self.store.get_request_executor(),
            lambda: None,
            self.store.conventions,
            response.operation_id,
            response.operation_node_tag,
        )
        operation.wait_for_completion()
        with self.store.open_session() as session:
            result = session.load_starting_with(Patch, "patches")
            values = list(map(lambda patch: patch.patched, result))
            self.assertEqual(3, len(result))
            self.assertEqual(3, len(values))
            for v in values:
                self.assertTrue(v)

    @unittest.skip("Exception dispatcher")
    def test_fail_patch_wrong_index_name(self):
        options = QueryOperationOptions(allow_stale=False, retrieve_details=True)
        query = IndexQuery("from index 'None' update {{this.name='NotExist'}}")
        query.wait_for_non_stale_results = True
        operation = PatchByQueryOperation(
            query_to_update=query,
            options=options,
        )
        with self.assertRaises(InvalidOperationException):
            response = self.store.operations.send(operation)
            if response:
                operation = NewOperation(
                    self.store.get_request_executor(),
                    lambda: None,
                    self.store.conventions,
                    response.operation_id,
                    response.operation_node_tag,
                )
                operation.wait_for_completion()
            else:
                raise ErrorResponseException("Got empty or None response from the server")

    def test_delete_by_index(self):

        ind = IndexDefinition(name="Users")
        ind.maps = ["from doc in docs.Users select new {name=doc.name}"]
        self.store.maintenance.send(PutIndexesOperation(ind))

        with self.store.open_session() as session:
            session.store(User(name="delete", age=0), key="deletes/1-A")
            session.save_changes()
            # doing the query here to make sure the query won't be stale
            query_result = list(session.advanced.raw_query("FROM INDEX 'Users'").wait_for_non_stale_results())
            assert query_result

        index_query = IndexQuery(query="FROM INDEX 'Users' WHERE name='delete'")
        response = self.store.operations.send(DeleteByQueryOperation(index_query))
        operation = NewOperation(
            self.store.get_request_executor(),
            lambda: None,
            self.store.conventions,
            response.operation_id,
            response.operation_node_tag,
        )
        operation.wait_for_completion()
        with self.store.open_session() as session:
            result = session.load_starting_with(User, "users")
            self.assertEqual(1, len(result))
            self.assertNotEqual("delete", result[0].name)

    def test_patch_by_collection(self):
        with self.store.open_session() as session:
            session.store(Patch(patched=False))
            session.store(Patch(patched=False))
            session.store(Patch(patched=False))
            session.save_changes()

        index_query = IndexQuery("From Patches Update {{this.patched=true}}")
        index_query.wait_for_non_stale_results = True

        response = self.store.operations.send(PatchByQueryOperation(query_to_update=index_query))
        operation = NewOperation(
            self.store.get_request_executor(),
            lambda: None,
            self.store.conventions,
            response.operation_id,
            response.operation_node_tag,
        )
        operation.wait_for_completion()
        with self.store.open_session() as session:
            result = session.load_starting_with(Patch, "patches")
            values = map(lambda patch: patch.patched, result)

            for v in values:
                self.assertTrue(v)

    def test_delete_by_collection_(self):
        with self.store.open_session() as session:
            self.assertEqual(1, len(session.load_starting_with(User, "users")))
        index_query = IndexQuery("From Users")
        operation = DeleteByQueryOperation(query_to_delete=index_query)
        result = self.store.operations.send(operation)
        op = NewOperation(
            self.store.get_request_executor(),
            lambda: None,
            self.store.conventions,
            result.operation_id,
            result.operation_node_tag,
        )
        op.wait_for_completion()
        with self.store.open_session() as session:
            self.assertEqual(0, len(session.load_starting_with(User, "users")))


if __name__ == "__main__":
    unittest.main()
