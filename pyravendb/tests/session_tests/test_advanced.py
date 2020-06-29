from pyravendb.tests.test_base import TestBase
from pyravendb.data.indexes import IndexDefinition
from pyravendb.raven_operations.maintenance_operations import PutIndexesOperation
from pyravendb.custom_exceptions.exceptions import InvalidOperationException
import unittest
import os

parent_path = os.path.dirname(os.getcwd())
if not parent_path.endswith("tests"):
    parent_path += "\\tests"
OUT_PUT_FILE_PATH = f"{parent_path}\\output.txt"


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
            self.assertFalse(id.endswith('/'))

    def test_stream_query(self):
        maps = ("from user in docs.Users "
                "select new {"
                "name = user.name,"
                "age = user.age}")
        index_definition = IndexDefinition(name="UserByName", maps=maps)

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
                session.advanced.attachment.store(user, "my_text_file", binary_list, content_type="text/plain")
                session.save_changes()

        with self.store.open_session() as session:
            attachment = session.advanced.attachment.get("users/1-A", "my_text_file")
            self.assertIsNotNone(attachment)

    def test_put_attachment_with_store(self):
        with self.store.open_session() as session:
            session.store(User("Idan", 30), "users/1-A")
            session.save_changes()

        with self.store.open_session() as session:
            with open(OUT_PUT_FILE_PATH, "rb") as binary_list:
                session.store(User("Ilay", 4), "users/2-A")
                session.advanced.attachment.store("users/1-A", "my_text_file", binary_list, content_type="text/plain")
                session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/2-A")
            attachment = session.advanced.attachment.get("users/1-A", "my_text_file")
            self.assertIsNotNone(attachment)
            self.assertIsNotNone(user)
            self.assertTrue(user.name == "Ilay")

    def test_delete_attachment(self):
        with self.store.open_session() as session:
            session.store(User("Idan", 30), "users/1-A")
            session.save_changes()

        with self.store.open_session() as session:
            with open(OUT_PUT_FILE_PATH, "rb") as binary_list:
                session.advanced.attachment.store("users/1-A", "my_text_file", binary_list, content_type="text/plain")
                session.save_changes()

        with self.store.open_session() as session:
            attachment = session.advanced.attachment.get("users/1-A", "my_text_file")
            self.assertIsNotNone(attachment)
            session.advanced.attachment.delete("users/1-A", "my_text_file")
            session.save_changes()
            attachment = session.advanced.attachment.get("users/1-A", "my_text_file")
            self.assertIsNone(attachment)

    def test_try_delete_attachment_putted_in_the_same_session(self):
        with self.store.open_session() as session:
            session.store(User("Idan", 30), "users/1-A")
            session.save_changes()

        with self.store.open_session() as session:
            with open(OUT_PUT_FILE_PATH, "rb") as binary_list:
                session.advanced.attachment.store("users/1-A", "my_text_file", binary_list, content_type="text/plain")
                with self.assertRaises(InvalidOperationException):
                    session.advanced.attachment.delete("users/1-A", "my_text_file")


if __name__ == "__main__":
    unittest.main()
