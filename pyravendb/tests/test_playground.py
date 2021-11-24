import unittest

from pyravendb.documents import DocumentStore


class User:
    def __init__(self, Id, name):
        self.Id = Id
        self.name = name


class MyTestCase(unittest.TestCase):
    def test_something(self):
        with DocumentStore("http://127.0.0.1:8080", "NorthWindTest") as store:
            store.initialize()
            with store.open_session() as session:
                u = User("users/1", "Rioghan")
                session.store(u)
                session.save_changes()
                loaded = session.load(User, "users/1")
                self.assertEqual(u, loaded.popitem()[1])

                session.clear()

                loaded = session.load(User, "users/1").popitem()[1]
                self.assertEqual(u.Id, loaded.Id)
                self.assertEqual(u.name, loaded.name)


if __name__ == "__main__":
    unittest.main()
