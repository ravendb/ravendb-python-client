from ravendb.tests.test_base import TestBase

from dataclasses import dataclass
from typing import Optional


@dataclass
class User:
    Id: str
    name: str
    tag: Optional[str] = "test"


@dataclass
class UserResult:
    Id: str
    name: str


class TestIssue157(TestBase):
    def setUp(self):
        super(TestIssue157, self).setUp()

    def test_select_fields_is_not_ignoring_where_equals(self):
        user_id = "users/1"
        user_id2 = "users/2"
        user_id3 = "users/3"
        user_id4 = "users/4"

        with self.store.open_session() as session:
            u = User(user_id, "Gracjan")
            s = User(user_id2, "Marcin")
            e = User(user_id3, "Maciej")
            r = User(user_id4, "Oren")
            session.store(u, user_id)
            session.store(s, user_id2)
            session.store(e, user_id3)
            session.store(r, user_id4)
            session.save_changes()

        with self.store.open_session() as session:
            response = list(session.advanced.document_query(collection_name="Users").where_equals("id()", user_id))
            self.assertEqual(1, len(response))

        with self.store.open_session() as session:
            response = list(
                session.advanced.document_query(collection_name="Users")
                .where_equals("id()", user_id)
                .select_fields(UserResult)
            )
            self.assertEqual(1, len(response))

        with self.store.open_session() as session:
            response = list(
                session.advanced.document_query(collection_name="Users")
                .select_fields(UserResult)
                .where_equals("id()", user_id)
            )
            self.assertEqual(1, len(response))
