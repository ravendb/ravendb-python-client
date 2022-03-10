from ravendb.tests.test_base import TestBase


class Document:
    def __init__(self, key=None):
        self.Id = key


class TestRavenDB10641(TestBase):
    def setUp(self):
        super(TestRavenDB10641, self).setUp()

    def test_can_edit_objects_in_metadata(self):
        with self.store.open_session() as session:
            v = Document()
            session.store(v, "items/first")
            items = {"lang": "en"}
            session.get_metadata_for(v).update({"Items": items})
            session.save_changes()

        with self.store.open_session() as session:
            v = session.load("items/first", Document)
            metadata = session.get_metadata_for(v).get("Items")
            metadata.update({"lang": "sv"})
            session.save_changes()

        with self.store.open_session() as session:
            v = session.load("items/first", Document)
            metadata = session.get_metadata_for(v)
            metadata.update({"test": "123"})
            session.save_changes()

        with self.store.open_session() as session:
            v = session.load("items/first", Document)
            metadata = session.get_metadata_for(v)
            session.save_changes()

        with self.store.open_session() as session:
            v = session.load("items/first", Document)
            metadata = session.get_metadata_for(v)
            self.assertEqual("sv", metadata.get("Items").get("lang"))
            self.assertEqual("123", metadata.get("test"))
