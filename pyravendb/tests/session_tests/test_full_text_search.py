import unittest

from pyravendb.documents.indexes.definitions import IndexDefinition, IndexFieldOptions, FieldIndexing
from pyravendb.documents.operations.indexes import PutIndexesOperation
from pyravendb.documents.queries.misc import SearchOperator
from pyravendb.tests.test_base import TestBase
from datetime import datetime


class LastFm(object):
    def __init__(self, artist="", track_id="", title="", datetime_time=None, tags=None):
        self.artist = artist
        self.track_id = track_id
        self.title = title
        self.datetime_time = datetime_time if datetime_time is not None else datetime.now()
        self.tags = tags if tags is not None else []


class LastFmAnalyzed(object):
    class Result(object):
        def __init__(self, query):
            self.query = query

    def __init__(self):
        index_map = (
            "from song in docs.LastFms "
            "select new {"
            "query = new object[] {"
            "song.artist,"
            "((object)song.datetime_time),"
            "song.tags,"
            "song.title,"
            "song.track_id}}"
        )

        self.index_definition = IndexDefinition(
            name=LastFmAnalyzed.__name__,
            fields={"query": IndexFieldOptions(indexing=FieldIndexing.SEARCH)},
        )
        self.index_definition.maps = index_map

    def execute(self, store):
        store.maintenance.send(PutIndexesOperation(self.index_definition))


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

    @unittest.skip("Full text search")
    def test_full_text_search_one(self):
        with self.store.open_session() as session:
            query = list(
                session.query(
                    object_type=LastFm,
                    wait_for_non_stale_results=True,
                    index_name=LastFmAnalyzed.__name__,
                ).search("query", search_terms="Me")
            )
            self.assertTrue("Me" in str(query[0].title) and "Me" in str(query[1].title))

    @unittest.skip("Full text search")
    def test_full_text_search_two(self):
        with self.store.open_session() as session:
            query = list(
                session.query(
                    object_type=LastFm,
                    wait_for_non_stale_results=True,
                    index_name=LastFmAnalyzed.__name__,
                )
                .search("query", search_terms="Me")
                .search("query", search_terms="Bobo")
            )
            self.assertEqual(len(query), 3)

    @unittest.skip("Full text search")
    def test_full_text_search_with_boost(self):
        with self.store.open_session() as session:
            query = list(
                session.query(
                    object_type=LastFm,
                    wait_for_non_stale_results=True,
                    index_name=LastFmAnalyzed.__name__,
                )
                .search("query", search_terms="Me")
                .boost(10)
                .search("query", search_terms="Bobo")
                .boost(2)
            )
            self.assertTrue(
                "Me" in str(query[0].title) and "Me" in str(query[1].title) and str(query[2].title) == "Spanish Grease"
            )

            query = list(
                session.query(
                    object_type=LastFm,
                    wait_for_non_stale_results=True,
                    index_name=LastFmAnalyzed.__name__,
                )
                .search("query", search_terms="Me")
                .boost(2)
                .search("query", search_terms="Bobo")
                .boost(10)
            )
            self.assertTrue(
                "Me" in str(query[1].title) and "Me" in str(query[2].title) and str(query[0].title) == "Spanish Grease"
            )

    @unittest.skip("Full text search")
    def test_full_text_search_with_and_operator(self):
        with self.store.open_session() as session:
            query = list(
                session.query(
                    object_type=LastFm,
                    wait_for_non_stale_results=True,
                    index_name=LastFmAnalyzed.__name__,
                ).search("query", search_terms="Me Come", operator=SearchOperator.AND)
            )
            assert len(query) == 1
            self.assertEqual(query[0].title, "Come With Me")
