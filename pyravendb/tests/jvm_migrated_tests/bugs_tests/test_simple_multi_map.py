import datetime
from abc import ABC
from typing import Optional

from pyravendb.documents.indexes.index_creation import AbstractMultiMapIndexCreationTask
from pyravendb.tests.test_base import TestBase


class CatsAndDogs(AbstractMultiMapIndexCreationTask):
    def __init__(self):
        super().__init__()
        self._add_map("from cat in docs.Cats select new { cat.name }")
        self._add_map("from dog in docs.Dogs select new { dog.name }")


class HaveName(ABC):
    def __init__(self, name: Optional[str] = None):
        self.name = name


class Cat(HaveName):
    def __init__(self, name: Optional[str] = None):
        super().__init__(name)


class Dog(HaveName):
    def __init__(self, name: Optional[str] = None):
        super().__init__(name)


class TestSimpleMultiMap(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_query_using_multi_map(self):
        CatsAndDogs().execute(self.store)

        with self.store.open_session() as session:
            cat = Cat()
            cat.name = "Tom"

            dog = Dog()
            dog.name = "Oscar"

            session.store(cat)
            session.store(dog)
            session.save_changes()

        with self.store.open_session() as session:
            have_names = list(
                session.query_index_type(CatsAndDogs, HaveName)
                .wait_for_non_stale_results(datetime.timedelta(seconds=10))
                .order_by("name")
            )
            self.assertEqual(2, len(have_names))

            self.assertTrue(isinstance(have_names[0], Dog))
            self.assertTrue(isinstance(have_names[1], Cat))
