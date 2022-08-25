from ravendb.documents.session.in_memory_document_session_operations import RefEq
from ravendb.tests.test_base import TestBase
from dataclasses import dataclass


class Data:
    def __init__(self, data: str):
        self.data = data


@dataclass()
class TestData:
    data: str


class TestDocumentsByEntity(TestBase):
    def setUp(self):
        super(TestDocumentsByEntity, self).setUp()

    def test_ref_eq(self):
        data = TestData(data="classified")

        wrapped_data_1 = RefEq(data)
        wrapped_data_2 = RefEq(wrapped_data_1)

        self.assertTrue(wrapped_data_2 == wrapped_data_1)
        with self.assertRaises(TypeError):
            wrapped_data_1 == data

    def test_documents_by_entity_holder(self):
        with self.store.open_session() as session:
            # legacy, save_changes and load
            test_data_store = TestData("top secret")
            session.store(test_data_store, "data/1")
            session.save_changes()

        with self.store.open_session() as session:
            test_data_load = session.load("data/1")
            self.assertEqual(test_data_store, test_data_load)

            # pop and get
            self.assertIsNotNone(session._documents_by_entity.get(test_data_load))
            self.assertIsNotNone(session._documents_by_entity.pop(test_data_load))
            self.assertIsNone(session._documents_by_entity.get(test_data_load))
            self.assertEqual(0, len(session._documents_by_entity))

            session.save_changes()

        with self.store.open_session() as session:
            entry_one = TestData(data="first note")
            entry_snd = TestData(data="second note")
            entry_classic = Data("third note")

            session.store(entry_one, "data/1")
            session.store(entry_snd, "data/2")
            session.store(entry_classic, "data/3")
            session.save_changes()

        with self.store.open_session() as session:
            intel = session.load(["data/1", "data/2", "data/3"])

            self.assertEqual(3, len(session._documents_by_entity))  # __len__

            self.assertEqual(3, len([item for item in session._documents_by_entity]))  # __iter__

            entities_keys = [e.value.key for e in session._documents_by_entity]
            for data in intel:  # __contains__
                self.assertIn(data, entities_keys)

            session._documents_by_entity.clear()  # clear
            self.assertEqual(0, len(session._documents_by_entity))

            session.save_changes()
