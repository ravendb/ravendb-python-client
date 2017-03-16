from pyravendb.tests.test_base import TestBase
from pyravendb.data.indexes import IndexDefinition
from pyravendb.data.indexes import FieldIndexing, IndexFieldOptions
from pyravendb.d_commands.raven_commands import PutIndexesCommand
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

        self.index_definition = IndexDefinition(name=LastFmAnalyzed.__name__, index_map=index_map,
                                                fields={"query": IndexFieldOptions(indexing=FieldIndexing.analyzed)})

    def execute(self, request_executor):
        put_index_command = PutIndexesCommand(self.index_definition)
        request_executor.execute(put_index_command)


class FullTextSearchTest(TestBase):
    @classmethod
    def setUpClass(cls):
        super(FullTextSearchTest, cls).setUpClass()
        cls.document_store = DocumentStore(cls.default_url, cls.default_database)
        cls.document_store.initialize()
        LastFmAnalyzed().execute(cls.requests_executor)
        with cls.document_store.open_session() as session:
            session.store(LastFm("Tania Maria", "TRALPJJ128F9311763", "Come With Me", datetime.now()), "LastFms/1")
            session.store(LastFm("Meghan Trainor", "TRBCNGI128F42597B4", "Me Too", datetime.now()), "LastFms/2")
            session.store(LastFm("Willie Bobo", "TRAACNS128F14A2DF5", "Spanish Grease", datetime.now()), "LastFms/3")
            session.save_changes()

    def test_full_text_search_one(self):
        with self.document_store.open_session() as session:
            query = list(session.query(object_type=LastFm, wait_for_non_stale_results=True,
                                       index_name=LastFmAnalyzed.__name__).search("query", search_terms="Me"))
            self.assertTrue("Me" in str(query[0].title) and "Me" in str(query[1].title))

    def test_full_text_search_two(self):
        with self.document_store.open_session() as session:
            query = list(session.query(object_type=LastFm, wait_for_non_stale_results=True,
                                       index_name=LastFmAnalyzed.__name__).search("query", search_terms="Me").
                         search("query", search_terms="Bobo"))
            self.assertEqual(len(query), 3)

    def test_full_text_search_with_boost(self):
        with self.document_store.open_session() as session:
            query = list(session.query(object_type=LastFm, wait_for_non_stale_results=True,
                                       index_name=LastFmAnalyzed.__name__).search("query", search_terms="Me", boost=10).
                         search("query", search_terms="Bobo", boost=2))
            self.assertTrue("Me" in str(query[0].title) and "Me" in str(query[1].title) and str(
                query[2].title) == "Spanish Grease")
