from pyravendb.tests.test_base import TestBase
from pyravendb.data.indexes import IndexDefinition
from pyravendb.data.indexes import FieldIndexing
from pyravendb.store.document_store import DocumentStore
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
        index_map = ("from song in docs.LastFms "
                     "select new {"
                     "query = new object[] {"
                     "song.artist,"
                     "((object)song.datetime_time),"
                     "song.tags,"
                     "song.title,"
                     "song.track_id}}")

        indexes = {"query": FieldIndexing.analyzed}

        self.index_definition = IndexDefinition(index_map=index_map, indexes=indexes)

    def execute(self, store):
        store.database_commands.put_index(LastFmAnalyzed.__name__, self.index_definition, True)


class FullTextSearchTest(TestBase):
    @classmethod
    def setUpClass(cls):
        super(FullTextSearchTest, cls).setUpClass()
        cls.db.put("LastFms/1", {"artist": "Tania Maria", "track_id": "TRALPJJ128F9311763", "title": "Come With Me",
                                 "datetime_time": datetime.now(), "tags": None},
                   {"Raven-Entity-Name": "LastFms", "Raven-Python-Type": "full_text_search_test.LastFm"})
        cls.db.put("LastFms/2", {"artist": "Meghan Trainor", "track_id": "TRBCNGI128F42597B4", "title": "Me Too",
                                 "datetime_time": datetime.now(), "tags": None},
                   {"Raven-Entity-Name": "LastFms", "Raven-Python-Type": "full_text_search_test.LastFm"})
        cls.db.put("LastFms/3", {"artist": "Willie Bobo", "track_id": "TRAACNS128F14A2DF5", "title": "Spanish Grease",
                                 "datetime_time": datetime.now(), "tags": None},
                   {"Raven-Entity-Name": "LastFms", "Raven-Python-Type": "full_text_search_test.LastFm"})
        cls.document_store = DocumentStore(cls.default_url, cls.default_database)
        cls.document_store.initialize()
        LastFmAnalyzed().execute(cls.document_store)

    def test_full_text_search_one(self):
        with self.document_store.open_session() as session:
            query = list(session.query(object_type=LastFm, wait_for_non_stale_results=True,
                                       index_name=LastFmAnalyzed.__name__).search("query", search_terms="Me"))
            self.assertTrue(str(query[0].title) == "Come With Me" and str(query[1].title) == "Me Too")

    def test_full_text_search_two(self):
        with self.document_store.open_session() as session:
            query = list(session.query(object_type=LastFm, wait_for_non_stale_results=True,
                                       index_name=LastFmAnalyzed.__name__).search("query", search_terms="Me").search(
                "query", search_terms="Bobo"))
            self.assertEqual(len(query), 3)

    def test_full_text_search_with_boost(self):
        with self.document_store.open_session() as session:
            query = list(session.query(object_type=LastFm, wait_for_non_stale_results=True,
                                       index_name=LastFmAnalyzed.__name__).search("query", search_terms="Me").boost(10).
                         search("query", search_terms="Bobo").boost(2))
            self.assertTrue(str(query[0].title) == "Come With Me" and str(query[1].title) == "Me Too" and str(
                query[2].title) == "Spanish Grease")
