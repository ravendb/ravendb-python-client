from pyravendb.tests.test_base import TestBase, User


class TestRavenDB10566(TestBase):
    def setUp(self):
        super(TestRavenDB10566, self).setUp()

    def test_should_be_available(self):
        self.name = None

        def update_item(session, doc_id, entity):
            self.name = session.advanced.get_metadata_for(entity).get("Name")

        with self.store.open_session() as session:
            session.events.after_save_change = update_item
            user = User("Oren", 30)
            session.store(user, "users/oren")
            metadata = session.advanced.get_metadata_for(user)
            metadata.update({"Name": "FooBar"})
            session.save_changes()

        self.assertEqual("FooBar", self.name)
