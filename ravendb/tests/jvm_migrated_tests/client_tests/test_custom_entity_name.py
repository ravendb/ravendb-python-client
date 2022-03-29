from typing import List

from ravendb.documents.conventions.document_conventions import DocumentConventions
from ravendb.documents.store.definition import DocumentStore
from ravendb.tests.test_base import TestBase


class Car:
    def __init__(self, Id: str = None, manufacturer: str = None):
        self.Id = Id
        self.manufacturer = manufacturer


class User:
    def __init__(self, Id: str = None, car_id: str = None):
        self.Id = Id
        self.car_id = car_id


class TestCustomEntityName(TestBase):
    def setUp(self):
        super(TestCustomEntityName, self).setUp()

    @staticmethod
    def __get_chars() -> List[str]:
        basic_chars = [chr(i) for i in range(1, 31)]
        extra_chars = [
            "a",
            "-",
            "'",
            '"',
            "\\",
            "!",
            "@",
            "#",
            "$",
            "%",
            "^",
            "&",
            "*",
            "(",
            ")",
            "?",
            "/",
            "_",
            "+",
            "{",
            "}",
            "|",
            ":",
            "<",
            ">",
            "[",
            "]",
            ";",
            ",",
            ".",
        ]
        return basic_chars + extra_chars

    @staticmethod
    def __get_characters_to_test_with_special() -> List[str]:
        basic_chars = TestCustomEntityName.__get_chars()
        special_chars = [
            "Ā",
            "Ȁ",
            "Ѐ",
            "Ԁ",
            "؀",
            "܀",
            "ऀ",
            "ਅ",
            "ଈ",
            "అ",
            "ഊ",
            "ข",
            "ဉ",
            "ᄍ",
            "ሎ",
            "ጇ",
            "ᐌ",
            "ᔎ",
            "ᘀ",
            "ᜩ",
            "ᢹ",
            "ᥤ",
            "ᨇ",
        ]
        return basic_chars + special_chars

    def test_find_collection_name(self):
        for c in self.__get_characters_to_test_with_special():
            self.__test_when_collection_and_id_contain_special_chars(c)

    def _customize_store(self, store: DocumentStore) -> None:
        store.conventions.find_collection_name = (
            lambda clazz: "Test" + self.c + DocumentConventions.default_get_collection_name(clazz)
        )

    def __test_when_collection_and_id_contain_special_chars(self, c: str) -> None:
        if 14 <= ord(c) <= 31:
            return

        self.c = c
        with self.store.open_session() as session:
            car = Car()
            car.manufacturer = "BMW"
            session.store(car)
            user = User()
            user.car_id = car.Id
            session.store(user)
            session.save_changes()

        with self.store.open_session() as session:
            results = list(session.query_collection(self.store.conventions.find_collection_name(User), User))
            self.assertEqual(1, len(results))
