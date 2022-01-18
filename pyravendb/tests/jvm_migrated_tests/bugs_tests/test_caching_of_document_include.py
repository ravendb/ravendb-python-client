from typing import List

from pyravendb.tests.test_base import TestBase


class User:
    def __init__(
        self,
        Id: str = None,
        name: str = None,
        partner_id: str = None,
        email: str = None,
        tags: List[str] = None,
        age: int = None,
        active: bool = None,
    ):
        self.Id = Id
        self.name = name
        self.partner_id = partner_id
        self.email = email
        self.tags = tags
        self.age = age
        self.active = active


class CachingOfDocumentsIncludeTest(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_avoid_using_server_for_load_with_include_if_everything_is_in_session_cacheLazy(self):
        with self.store.open_session() as session:
            user = User(name="Ayende")
            session.store(user)

            partner = User(partner_id="users/1-A")
            session.store(partner)

            session.save_changes()

        with self.store.open_session() as session:
            user: User = session.load("users/2-A", User)
            session.load(user.partner_id)

            old = session.number_of_requests
            new_user = session.include("partner_id").load(User, "users/2-A")
            self.assertEqual(old, session.number_of_requests)
