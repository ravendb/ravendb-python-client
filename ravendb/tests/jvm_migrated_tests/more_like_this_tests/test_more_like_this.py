import random
from abc import ABC
from typing import Optional, List, Type, TypeVar

from ravendb import AbstractIndexCreationTask, DocumentStore
from ravendb.documents.indexes.definitions import FieldStorage, FieldTermVector, FieldIndexing
from ravendb.documents.queries.more_like_this import MoreLikeThisStopWords, MoreLikeThisOptions
from ravendb.tests.test_base import TestBase

_T = TypeVar("_T")
_T_Index = TypeVar("_T_Index")


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
    def __init__(self, body: str = None):
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

    @staticmethod
    def _get_data_list() -> List[Data]:
        items = [
            Data(body="This is a test. Isn't it great? I hope I pass my test!"),
            Data(body="I have a test tomorrow. I hate having a test"),
            Data(body="Cake is great"),
            Data(body="This document has the word test only once"),
            Data(body="test"),
            Data(body="test"),
            Data(body="test"),
            Data(body="test"),
        ]

        return items

    def _assert_more_like_this_has_matches_for(
        self, object_type: Type[_T], index_type: Type[_T_Index], store: DocumentStore, document_key: str
    ):
        with store.open_session() as session:
            options = MoreLikeThisOptions(fields=["body"])
            results = list(
                session.query_index_type(index_type, object_type).more_like_this(
                    lambda f: f.using_document(lambda b: b.where_equals("id()", document_key)).with_options(options)
                )
            )
            self.assertNotEqual(0, len(results))

    def test_can_get_results_using_term_vectors(self):
        Id: Optional[str] = None
        with self.store.open_session() as session:
            DataIndex(True, True).execute(self.store)

            data_list = self._get_data_list()
            for data in data_list:
                session.store(data)
            session.save_changes()

            Id = session.advanced.get_document_id(data_list[0])
            self.wait_for_indexing(self.store)

        self._assert_more_like_this_has_matches_for(Data, DataIndex, self.store, Id)

    def test_can_use_min_doc_freq_param(self):
        key = "datas/1-A"

        with self.store.open_session() as session:
            DataIndex().execute(self.store)

            def __factory(text: str):
                data = Data(body=text)
                session.store(data)

            __factory("This is a test. Isn't it great? I hope I pass my test!")
            __factory("I have a test tomorrow. I hate having a test")
            __factory("Cake is great.")
            __factory("This document has the word test only once")

            session.save_changes()

            self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            options = MoreLikeThisOptions(fields=["body"], minimum_document_frequency=2)
            results = list(
                session.query_index_type(DataIndex, Data).more_like_this(
                    lambda f: f.using_document(lambda x: x.where_equals("id()", key)).with_options(options)
                )
            )
            self.assertNotEqual(0, len(results))

    def test_each_field_should_use_correct_analyzer(self):
        key1 = "datas/1-A"

        with self.store.open_session() as session:
            DataIndex().execute(self.store)

            for i in range(10):
                data = Data()
                data.whitespace_analyzer_field = "bob@hotmail.com hotmail"
                session.store(data)

            session.save_changes()
            self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            options = MoreLikeThisOptions(minimum_term_frequency=2, minimum_document_frequency=5)
            result = list(
                session.query_index_type(DataIndex, Data).more_like_this(
                    lambda f: f.using_document(lambda x: x.where_equals("id()", key1)).with_options(options)
                )
            )

            self.assertEqual(0, len(result))

        key2 = "datas/11-A"

        with self.store.open_session() as session:
            DataIndex().execute(self.store)

            for i in range(10):
                data = Data(whitespace_analyzer_field="bob@hotmail.com bob@hotmail.com")
                session.store(data)

            session.save_changes()
            self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            results = list(
                session.query_index_type(DataIndex, Data).more_like_this(
                    lambda f: f.using_document(lambda x: x.where_equals("id()", key2))
                )
            )
            self.assertNotEqual(0, len(results))

    @staticmethod
    def _get_lorem(num_words: int) -> str:
        the_lorem = (
            "Morbi nec purus eu libero interdum laoreet Nam metus quam posuere in elementum eget egestas eget "
            "justo Aenean orci ligula ullamcorper nec convallis non placerat nec lectus Quisque convallis "
            "porta suscipit Aliquam sollicitudin ligula sit amet libero cursus egestas Maecenas nec mauris "
            "neque at faucibus justo Fusce ut orci neque Nunc sodales pulvinar lobortis Praesent dui tellus "
            "fermentum sed faucibus nec faucibus non nibh Vestibulum adipiscing porta purus ut varius mi "
            "pulvinar eu Nam sagittis sodales hendrerit Vestibulum et tincidunt urna Fusce lacinia nisl at "
            "luctus lobortis lacus quam rhoncus risus a posuere nulla lorem at nisi Sed non erat nisl Cras in "
            "augue velit a mattis ante Etiam lorem dui elementum eget facilisis vitae viverra sit amet tortor "
            "Suspendisse potenti Nunc egestas accumsan justo viverra viverra Sed faucibus ullamcorper "
            "mauris ut pharetra ligula ornare eget Donec suscipit luctus rhoncus Pellentesque eget justo ac "
            "nunc tempus consequat Nullam fringilla egestas leo Praesent condimentum laoreet magna vitae "
            "luctus sem cursus sed Mauris massa purus suscipit ac malesuada a accumsan non neque Proin et "
            "libero vitae quam ultricies rhoncus Praesent urna neque molestie et suscipit vestibulum iaculis "
            "ac nulla Integer porta nulla vel leo ullamcorper eu rhoncus dui semper Donec dictum dui"
        )

        lorem_array = the_lorem.split(" ")
        output = []
        for i in range(num_words):
            output.append(lorem_array[random.randint(0, len(lorem_array) - 1)])
            output.append(" ")

        return "".join(output)

    def test_with_lots_of_random_data(self):
        key = "datas/1-A"

        with self.store.open_session() as session:
            DataIndex().execute(self.store)

            for i in range(100):
                data = Data(body=self._get_lorem(200))
                session.store(data)

            session.save_changes()

            self.wait_for_indexing(self.store)

        self._assert_more_like_this_has_matches_for(Data, DataIndex, self.store, key)

    def test_can_make_dynamic_document_queries(self):
        DataIndex().execute(self.store)

        with self.store.open_session() as session:
            data_list = self._get_data_list()

            for d in data_list:
                session.store(d)

            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            options = MoreLikeThisOptions(fields=["body"], minimum_term_frequency=1, minimum_document_frequency=1)

            results = list(
                session.query_index_type(DataIndex, Data).more_like_this(
                    lambda f: f.using_document('{ "body": "A test"}').with_options(options)
                )
            )
            self.assertEqual(7, len(results))

    def test_can_get_results_using_term_vectors_and_storage(self):
        Id: Optional[str] = None
        with self.store.open_session() as session:
            DataIndex(True, True).execute(self.store)

            data_list = self._get_data_list()
            for d in data_list:
                session.store(d)

            session.save_changes()

            Id = session.advanced.get_document_id(data_list[0])
            self.wait_for_indexing(self.store)

        self._assert_more_like_this_has_matches_for(Data, DataIndex, self.store, Id)

    def test_can_get_results_using_storage(self):
        Id: Optional[str] = None
        with self.store.open_session() as session:
            DataIndex(False, True).execute(self.store)

            data_list = self._get_data_list()
            for d in data_list:
                session.store(d)

            session.save_changes()

            Id = session.advanced.get_document_id(data_list[0])
            self.wait_for_indexing(self.store)

        self._assert_more_like_this_has_matches_for(Data, DataIndex, self.store, Id)

    def test_can_make_dynamic_document_queries_with_complex_properties(self):
        ComplexDataIndex().execute(self.store)

        with self.store.open_session() as session:
            complex_property = ComplexProperty("test")
            complex_data = ComplexData(property_=complex_property)

            session.store(complex_data)
            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            options = MoreLikeThisOptions(minimum_term_frequency=1, minimum_document_frequency=1)

            results = list(
                session.query_index_type(ComplexDataIndex, ComplexData).more_like_this(
                    lambda f: f.using_document('{ "Property": { "Body": "test" } }').with_options(options)
                )
            )
            self.assertEqual(1, len(results))

    def test_do_not_pass_field_names(self):
        key = "datas/1-A"

        with self.store.open_session() as session:
            DataIndex().execute(self.store)

            for i in range(10):
                data = Data(body=f"Body {i}", whitespace_analyzer_field="test test")
                session.store(data)

            session.save_changes()
            self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            results = list(
                session.query_index_type(DataIndex, Data).more_like_this(
                    lambda f: f.using_document(lambda x: x.where_equals("id()", key))
                )
            )
            self.assertNotEqual(0, len(results))

    def test_can_get_results_using_term_vectors_lazy(self):
        Id: Optional[str] = None
        with self.store.open_session() as session:
            DataIndex(True, False).execute(self.store)

            data_list = self._get_data_list()
            for d in data_list:
                session.store(d)

            session.save_changes()

            Id = session.advanced.get_document_id(data_list[0])
            self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            options = MoreLikeThisOptions(fields=["body"])
            lazy_list = (
                session.query_index_type(DataIndex, Data)
                .more_like_this(lambda f: f.using_document(lambda b: b.where_equals("id()", Id)).with_options(options))
                .lazily()
            )

            results = lazy_list.value

            self.assertNotEqual(0, len(results))

    def test_can_get_results_using_term_vector_with_document_query(self):
        Id: Optional[str] = None

        with self.store.open_session() as session:
            DataIndex(True, False).execute(self.store)

            data_list = self._get_data_list()
            for d in data_list:
                session.store(d)

            session.save_changes()

            Id = session.advanced.get_document_id(data_list[0])
            self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            options = MoreLikeThisOptions(fields=["body"])

            results = list(
                session.query_index_type(DataIndex, Data).more_like_this(
                    lambda f: f.using_document(lambda x: x.where_equals("id()", Id)).with_options(options)
                )
            )

            self.assertNotEqual(0, len(results))
