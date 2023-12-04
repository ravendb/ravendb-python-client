from ravendb.primitives import constants
from ravendb.documents.indexes.definitions import IndexDefinition
from ravendb.documents.operations.indexes import PutIndexesOperation
from ravendb.tests.test_base import TestBase
from ravendb.exceptions.exceptions import InvalidOperationException
import unittest
import pathlib
import os

parent_path = pathlib.Path(__file__).parent.resolve()
OUT_PUT_FILE_PATH = os.path.join(parent_path, "..", "..", "tests", "output.txt")


class User:
    def __init__(self, name, age):
        self.name = name
        self.age = age


class TestAdvanced(TestBase):
    def tearDown(self):
        super(TestAdvanced, self).tearDown()
        self.delete_all_topology_files()

    def test_get_document_id_after_save(self):
        with self.store.open_session() as s:
            user = User("U", 1)
            s.store(user, "test/")
            s.save_changes()
            id = s.advanced.get_document_id(user)
            self.assertFalse(id.endswith("/"))

    @unittest.skip("Query streaming")
    def test_stream_query(self):
        maps = "from user in docs.Users " "select new {" "name = user.name," "age = user.age}"
        index_definition = IndexDefinition()
        index_definition.name = "UserByName"
        index_definition.maps = [maps]

        self.store.maintenance.send(PutIndexesOperation(index_definition))

        with self.store.open_session() as session:
            for i in range(0, 12000):
                session.store(User("Idan", i))
            session.save_changes()

        with self.store.open_session() as session:
            query = session.query(object_type=User, index_name="UserByName")
            results = session.advanced.stream(query)
            result_counter = 0
            for _ in results:
                result_counter += 1
            self.assertTrue(result_counter == 12000)

    def test_put_attachment(self):
        with self.store.open_session() as session:
            session.store(User("Idan", 30), "users/1-A")
            session.save_changes()

        with self.store.open_session() as session:
            with open(OUT_PUT_FILE_PATH, "rb") as binary_list:
                user = session.load("users/1-A")
                session.advanced.attachments.store(user, "my_text_file", binary_list, content_type="text/plain")
                session.save_changes()

        with self.store.open_session() as session:
            attachment = session.advanced.attachments.get("users/1-A", "my_text_file")
            self.assertIsNotNone(attachment)

    def test_put_attachment_with_store(self):
        with self.store.open_session() as session:
            session.store(User("Idan", 30), "users/1-A")
            session.save_changes()

        with self.store.open_session() as session:
            with open(OUT_PUT_FILE_PATH, "rb") as binary_list:
                session.store(User("Ilay", 4), "users/2-A")
                session.advanced.attachments.store("users/1-A", "my_text_file", binary_list, content_type="text/plain")
                session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/2-A")
            attachment = session.advanced.attachments.get("users/1-A", "my_text_file")
            self.assertIsNotNone(attachment)
            self.assertIsNotNone(user)
            self.assertTrue(user.name == "Ilay")

    def test_delete_attachment(self):
        with self.store.open_session() as session:
            session.store(User("Idan", 30), "users/1-A")
            session.save_changes()

        with self.store.open_session() as session:
            with open(OUT_PUT_FILE_PATH, "rb") as binary_list:
                session.advanced.attachments.store("users/1-A", "my_text_file", binary_list, content_type="text/plain")
                session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1-A")
            metadata = session.advanced.get_metadata_for(user)
            attachments = metadata.metadata.get(constants.Documents.Metadata.ATTACHMENTS, None)
            attachment_names = []
            if attachments:
                attachment_names = list(map(lambda x: x.metadata["Name"], attachments))
            self.assertIn("my_text_file", attachment_names)

            session.advanced.attachments.delete("users/1-A", "my_text_file")
            session.save_changes()

            user = session.load("users/1-A")
            metadata = session.advanced.get_metadata_for(user)
            attachments = metadata.metadata.get(constants.Documents.Metadata.ATTACHMENTS, None)
            attachment_names = []
            if attachments:
                attachment_names = list(map(lambda x: x.metadata["Name"], attachments))
            self.assertNotIn("my_text_file", attachment_names)

    def test_try_delete_attachment_putted_in_the_same_session(self):
        with self.store.open_session() as session:
            session.store(User("Idan", 30), "users/1-A")
            session.save_changes()

        with self.store.open_session() as session:
            with open(OUT_PUT_FILE_PATH, "rb") as binary_list:
                session.advanced.attachments.store("users/1-A", "my_text_file", binary_list, content_type="text/plain")
                with self.assertRaises(InvalidOperationException):
                    session.advanced.attachments.delete("users/1-A", "my_text_file")


if __name__ == "__main__":
    unittest.main()
