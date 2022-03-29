from ravendb.documents.indexes.definitions import FieldIndexing
from ravendb.documents.indexes.index_creation import AbstractIndexCreationTask
from ravendb.tests.test_base import TestBase


class Animal:
    def __init__(self, animal_type: str = None, name: str = None):
        self.animal_type = animal_type
        self.name = name


class AnimalIndex(AbstractIndexCreationTask):
    def __init__(self):
        super(AnimalIndex, self).__init__()
        self.map = "from animal in docs.Animals select new { name = animal.name, type = animal.type }"
        self._analyze("name", "StandardAnalyzer")
        self._index("name", FieldIndexing.SEARCH)


class TestRavenD5669(TestBase):
    def setUp(self):
        super(TestRavenD5669, self).setUp()

    def store_animals(self):
        with self.store.open_session() as session:
            animal1 = Animal("Dog", "Peter Pan")
            animal2 = Animal("Dog", "Peter Poo")
            animal3 = Animal("Dog", "Peter Foo")
            session.store(animal1)
            session.store(animal2)
            session.store(animal3)
            session.save_changes()

        self.wait_for_indexing(self.store, self.store.database)

    def test_working_test_with_subclause(self):
        self.store.execute_index(AnimalIndex())
        self.store_animals()

        with self.store.open_session() as session:
            query = session.advanced.document_query_from_index_type(AnimalIndex, Animal)

            query.open_subclause()
            query = query.where_equals("type", "Cat")
            query = query.or_else()

            query.open_subclause()
            query = query.search("name", "Pan*")
            query = query.and_also()
            query = query.search("name", "Peter*")
            query = query.close_subclause()

            query.close_subclause()

            results = list(query)
            self.assertEqual(1, len(results))

    def test_working_test_with_different_search_term_order(self):
        self.store.execute_index(AnimalIndex())
        self.store_animals()

        with self.store.open_session() as session:
            query = session.advanced.document_query_from_index_type(AnimalIndex, Animal)

            query.open_subclause()
            query = query.where_equals("type", "Cat")
            query = query.or_else()
            query = query.search("name", "Peter*")
            query = query.and_also()
            query = query.search("name", "Pan*")

            query.close_subclause()

            results = list(query)

            self.assertEqual(1, len(results))
