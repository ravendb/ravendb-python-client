import unittest

from ravendb.documents.commands.crud import PutDocumentCommand
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase
from ravendb.tools.utils import Utils


class Article:
    def __init__(
        self,
        Id: str = None,
        title: str = None,
    ):
        self.Id = Id
        self.title = title


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

    # @unittest.skip("todo: Not passing on CI/CD")
    def test_can_put_document_using_command_with_surrogate_pairs(self):
        name_with_emojis = "Gracjan 😡😡🤬😀"

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

    def test_can_put_document_using_command_with_utf_8_chars(self):
        title_with_emojis = (
            "Déposer un CAPITAL SOCIAL : ce que tu dois ABSOLUMENT comprendre avant de lancer une ENTREPRISE 🏦"
        )

        article = Article(title=title_with_emojis)
        node = Utils.entity_to_dict(article, self.store.conventions.json_default_method)
        command = PutDocumentCommand("articles/1", None, node)
        self.store.get_request_executor().execute_command(command)

        result = command.result

        self.assertIsNotNone(result.change_vector)

        with self.store.open_session() as session:
            loaded_article = session.load("articles/1", Article)
            self.assertEqual(loaded_article.title, title_with_emojis)
