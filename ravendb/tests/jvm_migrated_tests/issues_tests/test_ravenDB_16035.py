from ravendb import DocumentStore
from ravendb.documents.session.event_args import SucceedRequestEventArgs
from ravendb.tests.test_base import TestBase


class User:
    def __init__(self, name: str = None):
        self.name = name


class TestRavenDB16035(TestBase):
    clear_cache = False

    def setUp(self):
        super(TestRavenDB16035, self).setUp()

    def _customize_store(self, store: DocumentStore) -> None:
        def __event(args: SucceedRequestEventArgs):
            if TestRavenDB16035.clear_cache:
                store.get_request_executor().cache.clear()

        store.add_on_succeed_request(__event)

    def test_can_mix_lazy_and_aggressive_caching(self):
        with self.store.open_session() as session:
            user = User()
            user.name = "Arava"
            session.store(user)
            session.save_changes()

        with self.store.open_session() as session:
            l1 = session.query(object_type=User).where_equals("name", "Arava").lazily()
            l2 = session.query(object_type=User).where_equals("name", "Phoebe").lazily()
            l3 = session.query(object_type=User).where_exists("name").count_lazily()

            self.assertNotEqual(0, len(l1.value))
            self.assertEqual(0, len(l2.value))
            self.assertEqual(1, l3.value)

        with self.store.open_session() as session:
            TestRavenDB16035.clear_cache = True

        with self.store.open_session() as session:
            l1 = session.query(object_type=User).where_equals("name", "Arava").lazily()
            l2 = session.query(object_type=User).where_equals("name", "Phoebe").lazily()
            l3 = session.query(object_type=User).where_exists("name").count_lazily()

            self.assertNotEqual(0, len(l1.value))
            self.assertEqual(0, len(l2.value))
            self.assertEqual(1, l3.value)
