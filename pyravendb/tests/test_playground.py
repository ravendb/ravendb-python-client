import unittest
import pyravendb.tests.test_base


class User:
    def __init__(self, Id, name):
        self.Id = Id
        self.name = name


class MyTest(pyravendb.tests.test_base.TestBase):
    def test_setup(self):
        with self.store.open_session() as session:
            u = User("users/1", "Rioghan")
            session.store(u)
            session.save_changes()
            loaded = session.load("users/1", User)
            self.assertEqual(u, loaded)

            session.clear()

            loaded = session.load("users/1", User)
            self.assertEqual(u.Id, loaded.Id)
            self.assertEqual(u.name, loaded.name)
