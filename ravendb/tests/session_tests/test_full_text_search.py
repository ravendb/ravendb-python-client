from ravendb.documents.indexes.definitions import FieldIndexing
from ravendb.documents.indexes.abstract_index_creation_tasks import AbstractIndexCreationTask
from ravendb.documents.queries.misc import SearchOperator
from ravendb.tests.test_base import TestBase
from datetime import datetime


class LastFm(object):
    def __init__(self, artist="", track_id="", title="", datetime_time=None, tags=None):
        self.artist = artist
        self.track_id = track_id
        self.title = title
        self.datetime_time = datetime_time if datetime_time is not None else datetime.now()
        self.tags = tags if tags is not None else []


class LastFmAnalyzed(AbstractIndexCreationTask):
    class Result(object):
        def __init__(self, query):
            self.query = query

    def __init__(self):
        super().__init__()
        self.map = (
            "from song in docs.LastFms "
            "select new {"
            "query = new object[] {"
            "song.artist,"
            "((object)song.datetime_time),"
            "song.tags,"
            "song.title,"
            "song.track_id}}"
        )
        self._analyze("query", "StandardAnalyzer")
        self._index("query", FieldIndexing.SEARCH)


class FullTextSearchTest(TestBase):
    def setUp(self):
        super(FullTextSearchTest, self).setUp()
        LastFmAnalyzed().execute(self.store)
        with self.store.open_session() as session:
            session.store(
                LastFm("Tania Maria", "TRALPJJ128F9311763", "Come With Me", datetime.now()),
                "LastFms/1",
            )
            session.store(
                LastFm("Meghan Trainor", "TRBCNGI128F42597B4", "Me Too", datetime.now()),
                "LastFms/2",
            )
            session.store(
                LastFm(
                    "Willie Bobo",
                    "TRAACNS128F14A2DF5",
                    "Spanish Grease",
                    datetime.now(),
                ),
                "LastFms/3",
            )
            session.save_changes()

    def tearDown(self):
        super(FullTextSearchTest, self).tearDown()
        self.delete_all_topology_files()

    def test_full_text_search_one(self):
        with self.store.open_session() as session:
            query = list(
                session.query_index_type(LastFmAnalyzed, LastFm).wait_for_non_stale_results().search("query", "Me")
            )
            self.assertTrue("Me" in str(query[0].title) and "Me" in str(query[1].title))

    def test_full_text_search_two(self):
        with self.store.open_session() as session:
            query = list(
                session.query_index_type(
                    LastFmAnalyzed,
                    LastFm,
                )
                .wait_for_non_stale_results()
                .search("query", "Me")
                .or_else()
                .search("query", "Bobo")
            )
            self.assertEqual(len(query), 3)

    def test_full_text_search_with_boost(self):
        with self.store.open_session() as session:
            query = list(
                session.query_index_type(
                    LastFmAnalyzed,
                    LastFm,
                )
                .wait_for_non_stale_results()
                .search("query", "Me")
                .boost(10)
                .or_else()
                .search("query", "Bobo")
                .boost(2)
            )
            self.assertTrue(
                "Me" in str(query[0].title) and "Me" in str(query[1].title) and str(query[2].title) == "Spanish Grease"
            )

            query = list(
                session.query_index_type(
                    LastFmAnalyzed,
                    LastFm,
                )
                .wait_for_non_stale_results()
                .search("query", search_terms="Me")
                .boost(2)
                .or_else()
                .search("query", search_terms="Bobo")
                .boost(10)
            )
            self.assertTrue(
                "Me" in str(query[1].title) and "Me" in str(query[2].title) and str(query[0].title) == "Spanish Grease"
            )

    def test_full_text_search_with_and_operator(self):
        with self.store.open_session() as session:
            query = list(
                session.query_index_type(
                    LastFmAnalyzed,
                    LastFm,
                )
                .wait_for_non_stale_results()
                .search("query", "Me Come", SearchOperator.AND)
            )
            assert len(query) == 1
            self.assertEqual(query[0].title, "Come With Me")
