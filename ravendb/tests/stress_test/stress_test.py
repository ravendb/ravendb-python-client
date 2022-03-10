from ravendb.tests.test_base import TestBase


class User:
    def __init__(self, name):
        self.name = name


class StressTest(TestBase):
    def setUp(self):
        super(StressTest, self).setUp()

    def tearDown(self):
        super(StressTest, self).tearDown()
        self.delete_all_topology_files()

    def test_for_max_document(self):
        with self.store.open_session() as session:
            for i in range(0, 600000):
                session.store(User("Or"))
            session.save_changes()
