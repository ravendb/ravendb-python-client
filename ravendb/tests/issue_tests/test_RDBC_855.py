from datetime import datetime

from pydantic import BaseModel

from ravendb.tests.test_base import TestBase


class User(BaseModel):
    name: str = None
    birthday: datetime = None
    Id: str = None


class TestRDBC855(TestBase):
    def test_storing_pydantic_objects(self):
        with self.store.open_session() as session:
            session.store(User(name="Josh", birthday=datetime(1999, 1, 1), Id="users/51"))
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/51", User)
            self.assertEqual("Josh", user.name)
