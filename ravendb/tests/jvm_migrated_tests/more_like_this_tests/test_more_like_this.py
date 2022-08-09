from abc import ABC

from ravendb import AbstractIndexCreationTask
from ravendb.documents.indexes.definitions import FieldStorage, FieldTermVector, FieldIndexing
from ravendb.documents.queries.more_like_this import MoreLikeThisStopWords, MoreLikeThisOptions
from ravendb.tests.test_base import TestBase


class Identity(ABC):
    def __init__(self, Id: str = None):
        self.Id = Id


class Data(Identity):
    def __init__(self, Id: str = None, body: str = None, whitespace_analyzer_field: str = None, person_id: str = None):
        super(Data, self).__init__(Id)
        self.body = body
        self.whitespace_analyzer_field = whitespace_analyzer_field
        self.person_id = person_id


class ComplexProperty:
    def __init__(self, body: str):
        self.body = body


class ComplexData:
    def __init__(self, Id: str = None, property_: ComplexProperty = None):
        self.Id = Id
        self.property_ = property_


class DataIndex(AbstractIndexCreationTask):
    def __init__(self, term_vector: bool = True, store: bool = False):
        super(DataIndex, self).__init__()
        self.map = "from doc in docs.Datas select new { doc.body, doc.whitespace_analyzer_field }"
        self._analyze("body", "Lucene.Net.Analysis.Standard.StandardAnalyzer")
        self._analyze("whitespace_analyzer_field", "Lucene.Net.Analysis.WhitespaceAnalyzer")

        if store:
            self._store("body", FieldStorage.YES)
            self._store("whitespace_analyzer_field", FieldStorage.YES)

        if term_vector:
            self._term_vector("body", FieldTermVector.YES)
            self._term_vector("whitespace_analyzer_field", FieldTermVector.YES)


class ComplexDataIndex(AbstractIndexCreationTask):
    def __init__(self):
        super(ComplexDataIndex, self).__init__()
        self.map = "from doc in docs.ComplexDatas select new { doc.property_, doc.property_.body }"
        self._index("body", FieldIndexing.SEARCH)


class TestMoreLikeThis(TestBase):
    def setUp(self):
        super(TestMoreLikeThis, self).setUp()

    def test_can_use_stop_words(self):
        key = "datas/1-A"
        DataIndex().execute(self.store)
        with self.store.open_session() as session:

            def __factory(text: str):
                data = Data(body=text)
                session.store(data)

            __factory("This is a test. Isn't it great? I hope I pass my test!")
            __factory("I should not hit this document. I hope")
            __factory("Cake is great.")
            __factory("This document has the word test only once")
            __factory("test")
            __factory("test")
            __factory("test")
            __factory("test")

            stop_words = MoreLikeThisStopWords("Config/Stopwords", ["I", "A", "Be"])
            session.store(stop_words)

            session.save_changes()
            self.wait_for_indexing(self.store)

        index_name = DataIndex().index_name

        with self.store.open_session() as session:
            options = MoreLikeThisOptions(
                minimum_term_frequency=2, minimum_document_frequency=1, stop_words_document_id="Config/Stopwords"
            )
            results = list(
                session.query_index_type(DataIndex, Data).more_like_this(
                    lambda f: f.using_document(lambda x: x.where_equals("id()", key)).with_options(options)
                )
            )
            self.assertEqual(5, len(results))

    def test_can_use_boost_param(self):
        key = "datas/1-A"

        with self.store.open_session() as session:
            DataIndex().execute(self.store)

            def __factory(text: str):
                data = Data(body=text)
                session.store(data)

            __factory("This is a test. It is a great test . I hope I pass my great test!")
            __factory("Cake is great.")
            __factory("I have a test tomorrow.")

            session.save_changes()

            self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            options = MoreLikeThisOptions(
                fields=["body"],
                minimum_word_length=3,
                minimum_document_frequency=1,
                minimum_term_frequency=2,
                boost=True,
            )

            results = list(
                session.query_index_type(DataIndex, Data).more_like_this(
                    lambda f: f.using_document(lambda x: x.where_equals("id()", key)).with_options(options)
                )
            )

            self.assertNotEqual(0, len(results))
            self.assertEqual("I have a test tomorrow.", results[0].body)
