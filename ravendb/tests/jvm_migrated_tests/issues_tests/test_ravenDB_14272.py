from typing import Dict

from ravendb import DocumentStore, QueryData
from ravendb.tests.test_base import TestBase


class TalkUserDef:
    def __init__(self, a: str = None):
        self.a = a


class TalkUserIds:
    def __init__(self, user_defs: Dict[str, TalkUserDef]):
        self.user_defs = user_defs


class UserTalk:
    def __init__(self, user_defs: Dict[str, TalkUserDef], name: str):
        self.user_defs = user_defs
        self.name = name


class TestRavenDB14272(TestBase):
    def setUp(self):
        super(TestRavenDB14272, self).setUp()

    def _save_user_talk(self, store: DocumentStore):
        user_talk = UserTalk({"test1": TalkUserDef(), "test2": TalkUserDef()}, "Grisha")

        with store.open_session() as session:
            session.store(user_talk)
            session.save_changes()

        return user_talk

    def test_select_fields_1(self):
        user_talk = self._save_user_talk(self.store)

        with self.store.open_session() as session:
            result = list(session.query(object_type=UserTalk).select_fields(TalkUserIds))
            self.assertEqual(1, len(result))
            self.assertEqual(2, len(result[0].user_defs))
            self.assertEqual(list(result[0].user_defs.keys()), list(user_talk.user_defs.keys()))

    def test_select_fields_2(self):
        user_talk = self._save_user_talk(self.store)

        with self.store.open_session() as session:
            query_data = QueryData(["user_defs"], ["user_defs"])

            result = list(session.query(object_type=UserTalk).select_fields_query_data(TalkUserIds, query_data))

            self.assertEqual(1, len(result))
            self.assertEqual(2, len(result[0].user_defs))
            self.assertEqual(list(result[0].user_defs.keys()), list(user_talk.user_defs.keys()))

    def test_select_fields_3(self):
        user_talk = self._save_user_talk(self.store)

        with self.store.open_session() as session:
            query_data = QueryData(["user_defs"], ["user_defs"])
            result = list(session.query(object_type=UserTalk).select_fields_query_data(TalkUserIds, query_data))
            self.assertEqual(1, len(result))
            self.assertEqual(2, len(result[0].user_defs))
            self.assertEqual(list(result[0].user_defs.keys()), list(user_talk.user_defs.keys()))

    def test_select_fields_4(self):
        user_talk = self._save_user_talk(self.store)

        with self.store.open_session() as session:
            result = list(session.query(object_type=UserTalk).select_fields(str, "name"))
            self.assertEqual(1, len(result))
            self.assertEqual(user_talk.name, result[0])

    def test_streaming_document_query_projection(self):
        user_talk = self._save_user_talk(self.store)
        with self.store.open_session() as session:
            query = session.advanced.document_query(object_type=UserTalk).select_fields(TalkUserIds, "user_defs")
            stream = session.advanced.stream(query)
            for result in stream:
                projection = result.document
                self.assertIsNotNone(projection)
                self.assertIsNotNone(projection.user_defs)
                self.assertEqual(2, len(projection.user_defs))
                self.assertSequenceContainsElements(user_talk.user_defs.keys(), *projection.user_defs.keys())

    def test_streaming_query_projection(self):
        user_talk = self._save_user_talk(self.store)
        with self.store.open_session() as session:
            query = session.query(object_type=UserTalk).select_fields(TalkUserIds)
            stream = session.advanced.stream(query)
            for result in stream:
                projection = result.document
                self.assertIsNotNone(projection)
                self.assertIsNotNone(projection.user_defs)
                self.assertEqual(2, len(projection.user_defs))
                self.assertSequenceContainsElements(user_talk.user_defs.keys(), *projection.user_defs.keys())
