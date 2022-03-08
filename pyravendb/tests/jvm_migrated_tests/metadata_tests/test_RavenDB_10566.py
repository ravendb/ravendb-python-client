from pyravendb.documents.session.event_args import AfterSaveChangesEventArgs
from pyravendb.tests.test_base import TestBase, User


class TestRavenDB10566(TestBase):
    def setUp(self):
        super(TestRavenDB10566, self).setUp()

    def test_should_be_available(self):
        self.name = None

        def update_item(after_save_changes_event_args: AfterSaveChangesEventArgs):
            self.name = session.get_metadata_for(after_save_changes_event_args.entity).get("Name")

        with self.store.open_session() as session:
            session.add_after_save_changes(update_item)
            user = User("Oren", 30)
            session.store(user, "users/oren")
            metadata = session.get_metadata_for(user)
            metadata.update({"Name": "FooBar"})
            session.save_changes()

        self.assertEqual("FooBar", self.name)
