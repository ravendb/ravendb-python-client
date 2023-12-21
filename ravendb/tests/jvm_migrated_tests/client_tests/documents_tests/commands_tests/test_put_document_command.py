from ravendb.documents.commands.crud import PutDocumentCommand
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase
from ravendb.tools.utils import Utils


class TestPutDocumentCommand(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_put_document_using_command(self):
        with self.store.open_session() as session:
            user = User(name="Gracjan", age=30)
            node = Utils.entity_to_dict(user, self.store.conventions.json_default_method)
            command = PutDocumentCommand("users/1", None, node)
            self.store.get_request_executor().execute_command(command)

            result = command.result

            self.assertEqual("users/1", result.key)

            self.assertIsNotNone(result.change_vector)

            with self.store.open_session() as session:
                loaded_user = session.load("users/1", User)
                self.assertEqual(loaded_user.name, "Gracjan")

    def test_can_put_document_using_command_with_surrogate_pairs(self):
        name_with_emojis = "Gracjan \uD83D\uDE21\uD83D\uDE21\uD83E\uDD2C\uD83D\uDE00😡😡🤬😀"

        user = User(name=name_with_emojis, age=31)
        node = Utils.entity_to_dict(user, self.store.conventions.json_default_method)
        command = PutDocumentCommand("users/2", None, node)
        self.store.get_request_executor().execute_command(command)

        result = command.result

        self.assertEqual("users/2", result.key)

        self.assertIsNotNone(result.change_vector)

        with self.store.open_session() as session:
            loaded_user = session.load("users/2", User)
            self.assertEqual(loaded_user.name, name_with_emojis)
