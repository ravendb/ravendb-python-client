from ravendb import AbstractIndexCreationTask
from ravendb.documents.indexes.definitions import FieldIndexing
from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase


class Fox:
    def __init__(self, name: str = None):
        self.name = name


class Fox_Search(AbstractIndexCreationTask):
    def __init__(self):
        super(Fox_Search, self).__init__()
        self.map = "from f in docs.Foxes select new { f.name }"
        self._index("name", FieldIndexing.SEARCH)


class TestRavenDB12030(TestBase):
    def setUp(self):
        super().setUp()

    def test_simple_proximity(self):
        Fox_Search().execute(self.store)
        with self.store.open_session() as session:
            f1 = Fox("a quick brown fox")
            f2 = Fox("the fox is quick")
            session.store(f1)
            session.store(f2)
            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            foxes = list(
                session.advanced.document_query_from_index_type(Fox_Search, Fox)
                .search("name", "quick_fox")
                .proximity(1)
            )
            self.assertEqual(1, len(foxes))
            self.assertEqual("a quick brown fox", foxes[0].name)

            foxes = list(
                session.advanced.document_query_from_index_type(Fox_Search, Fox)
                .search("name", "quick fox")
                .proximity(2)
            )
            self.assertEqual(2, len(foxes))
            self.assertEqual("a quick brown fox", foxes[0].name)
            self.assertEqual("the fox is quick", foxes[1].name)

    def test_simple_fuzzy(self):
        with self.store.open_session() as session:
            hr = Company()
            hr.name = "Hibernating Rhinos"
            session.store(hr)

            cf = Company()
            cf.name = "CodeForge"
            session.store(cf)

            session.save_changes()

        with self.store.open_session() as session:
            companies = list(
                session.advanced.document_query(object_type=Company).where_equals("name", "CoedForhe").fuzzy(0.5)
            )

            self.assertEqual(1, len(companies))
            self.assertEqual("CodeForge", companies[0].name)

            companies = list(
                session.advanced.document_query(object_type=Company)
                .where_equals("name", "Hiberanting Rinhos")
                .fuzzy(0.5)
            )

            self.assertEqual(1, len(companies))
            self.assertEqual("Hibernating Rhinos", companies[0].name)

            companies = list(
                session.advanced.document_query(object_type=Company).where_equals("name", "CoedForhe").fuzzy(0.99)
            )
            self.assertEqual(0, len(companies))
