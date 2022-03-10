from ravendb.tests.test_base import TestBase


class Company:
    def __init__(self, name):
        self.name = name


class TestRavenDB10641(TestBase):
    def setUp(self):
        super(TestRavenDB10641, self).setUp()

    def test_has_changes_should_detect_deletes(self):
        with self.store.open_session() as session:
            session.store(Company("HR"), "companies/1")
            session.save_changes()
        with self.store.open_session() as session:
            company = session.load("companies/1", Company)
            session.delete(company)

            changes = session.advanced.what_changed()
            self.assertEqual(1, len(changes))
            self.assertTrue(session.has_changes())
