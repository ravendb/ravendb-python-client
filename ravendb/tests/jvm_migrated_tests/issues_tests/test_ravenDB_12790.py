from ravendb import AbstractIndexCreationTask
from ravendb.exceptions.documents.indexes import IndexDoesNotExistException
from ravendb.tests.test_base import TestBase


class Document:
    def __init__(self, name: str = None):
        self.name = name


class DocumentIndex(AbstractIndexCreationTask):
    pass


class TestRavenDB12790(TestBase):
    def setUp(self):
        super(TestRavenDB12790, self).setUp()

    def test_lazy_query_against_missing_index(self):
        with self.store.open_session() as session:
            document = Document()
            document.name = "name"
            session.store(document)
            session.save_changes()

        # intentionally not creating the index that we query against

        with self.store.open_session() as session:
            with self.assertRaises(IndexDoesNotExistException):
                list(session.query_index_type(DocumentIndex, Document))

        with self.store.open_session() as session:
            lazy_query = session.query_index_type(DocumentIndex, Document).lazily()
            with self.assertRaises(IndexDoesNotExistException):
                lazy_query.value
