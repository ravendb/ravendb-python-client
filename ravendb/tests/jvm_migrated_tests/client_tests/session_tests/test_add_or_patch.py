from datetime import datetime
from dataclasses import dataclass

from typing import List

from ravendb.tests.test_base import TestBase
from ravendb.tools.utils import Utils


@dataclass
class User:
    last_login: datetime = None
    first_name: str = None
    last_name: str = None
    login_times: List[datetime] = None
    login_count: int = None


class TestAddOrPatch(TestBase):
    def setUp(self):
        super(TestAddOrPatch, self).setUp()

    def test_can_add_or_patch(self):
        key = "users/1"

        with self.store.open_session() as session:
            new_user = User(first_name="Hibernating", last_name="Rhinos", login_count=1)
            session.store(new_user, key)
            session.save_changes()

        with self.store.open_session() as session:
            new_user = User(first_name="Hibernating", last_name="Rhinos")
            session.advanced.add_or_increment(key, new_user, "login_count", 3)

            session.save_changes()

            self.assertEqual(1, session.advanced.number_of_requests)

            user = session.load(key, User)
            self.assertEqual(4, user.login_count)

            session.delete(key)
            session.save_changes()

        with self.store.open_session() as session:
            new_user = User(first_name="Hibernating", last_name="Rhinos", last_login=datetime.fromtimestamp(0))
            datetime_now = datetime.now()
            d1993 = datetime(
                1993,
                datetime_now.month,
                datetime_now.day,
                datetime_now.hour,
                datetime_now.minute,
                datetime_now.second,
                datetime_now.microsecond,
            )

            session.advanced.add_or_patch(key, new_user, "last_login", d1993)

            session.save_changes()

            self.assertEqual(1, session.advanced.number_of_requests)

            user = session.load(key, User)
            self.assertEqual("Hibernating", user.first_name)
            self.assertEqual("Rhinos", user.last_name)
            self.assertEqual(Utils.datetime_to_string(datetime.fromtimestamp(0)), user.last_login)  # todo: mappers?

    def test_can_add_or_patch_add_item_to_an_existing_array(self):
        key = "users/1"

        with self.store.open_session() as session:
            user = User(first_name="Hibernating", last_name="Rhinos")
            datetime_now = datetime.now()
            d2000 = datetime(
                2000,
                datetime_now.month,
                datetime_now.day,
                datetime_now.hour,
                datetime_now.minute,
                datetime_now.second,
                datetime_now.microsecond,
            )

            user.login_times = [d2000]
            session.store(user, key)
            session.save_changes()

            self.assertEqual(1, session.advanced.number_of_requests)

        with self.store.open_session() as session:
            new_user = User(first_name="Hibernating", last_name="Rhinos", login_times=[])
            d1993 = datetime(
                1993,
                datetime_now.month,
                datetime_now.day,
                datetime_now.hour,
                datetime_now.minute,
                datetime_now.second,
                datetime_now.microsecond,
            )

            d2000 = datetime(
                2000,
                datetime_now.month,
                datetime_now.day,
                datetime_now.hour,
                datetime_now.minute,
                datetime_now.second,
                datetime_now.microsecond,
            )

            session.advanced.add_or_patch_array(key, new_user, "login_times", lambda array: array.add(d1993, d2000))
            session.save_changes()

            self.assertEqual(1, session.advanced.number_of_requests)

            user = session.load(key, User)
            self.assertEqual(3, len(user.login_times))

            session.delete(key)
            session.save_changes()

        with self.store.open_session() as session:
            now = datetime.now()

            new_user = User()
            new_user.last_name = "Hibernating"
            new_user.first_name = "Rhinos"
            new_user.last_login = now

            d1993 = datetime(
                1993,
                datetime_now.month,
                datetime_now.day,
                datetime_now.hour,
                datetime_now.minute,
                datetime_now.second,
                datetime_now.microsecond,
            )

            session.advanced.add_or_patch(key, new_user, "last_login", d1993)

            session.save_changes()

            self.assertEqual(1, session.advanced.number_of_requests)

            user = session.load(key, User)
            self.assertEqual("Hibernating", user.last_name)
            self.assertEqual("Rhinos", user.first_name)
            self.assertEqual(Utils.datetime_to_string(now), user.last_login)

    def test_can_add_or_patch_increment(self):
        key = "users/1"
        with self.store.open_session() as session:
            new_user = User()
            new_user.first_name = "Hibernating"
            new_user.last_name = "Rhinos"
            new_user.login_count = 1

            session.store(new_user, key)
            session.save_changes()

        with self.store.open_session() as session:
            new_user = User()
            new_user.first_name = "Hibernating"
            new_user.last_name = "Rhinos"
            session.advanced.add_or_increment(key, new_user, "login_count", 3)
            session.save_changes()

            self.assertEqual(1, session.advanced.number_of_requests)

            user = session.load(key, User)
            self.assertEqual(4, user.login_count)

            session.delete(key)
            session.save_changes()

        with self.store.open_session() as session:
            new_user = User()
            new_user.first_name = "Hibernating"
            new_user.last_name = "Rhinos"
            new_user.last_login = datetime.fromtimestamp(0)
            datetime_now = datetime.now()
            d1993 = datetime(
                1993,
                datetime_now.month,
                datetime_now.day,
                datetime_now.hour,
                datetime_now.minute,
                datetime_now.second,
                datetime_now.microsecond,
            )

            session.advanced.add_or_patch(key, new_user, "last_login", d1993)

            session.save_changes()

            self.assertEqual(1, session.advanced.number_of_requests)

            user = session.load(key, User)
            self.assertEqual("Hibernating", user.first_name)
            self.assertEqual("Rhinos", user.last_name)
            self.assertEqual(Utils.datetime_to_string(datetime.fromtimestamp(0)), user.last_login)
