from ravendb.tests.test_base import TestBase


class TestDocument:
    def __init__(self, name: str = None):
        self.name = name


class TestRDBC940(TestBase):
    def setUp(self):
        super(TestRDBC940, self).setUp()

    def test_operations_with_zstandard_imported(self):
        import zstandard

        document_id = "test-documents/1"

        with self.store.open_session() as session:
            test_doc = TestDocument(name="Test Document")
            session.store(test_doc, document_id)
            session.save_changes()

        with self.store.open_session() as session:
            loaded_doc = session.load(document_id, TestDocument)

        with self.store.open_session() as session:
            session.delete(document_id)
            session.save_changes()
