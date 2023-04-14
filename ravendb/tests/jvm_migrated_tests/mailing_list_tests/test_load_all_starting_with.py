from ravendb.tests.test_base import TestBase


class Abc:
    def __init__(self, Id: str = None):
        self.Id = Id


class Xyz:
    def __init__(self, Id: str = None):
        self.Id = Id


class TestLoadAllStartingWith(TestBase):
    def setUp(self):
        super(TestLoadAllStartingWith, self).setUp()

    def test_load_all_starting_with(self):
        doc1 = Abc("abc/1")
        doc2 = Xyz("xyz/1")

        with self.store.open_session() as session:
            session.store(doc1)
            session.store(doc2)
            session.save_changes()

        with self.store.open_session() as session:
            test_classes = session.advanced.lazily.load_starting_with("abc/", Abc)

            test2_classes = session.query(object_type=Xyz).wait_for_non_stale_results().lazily().value
            self.assertEqual(1, len(test_classes.value))
            self.assertEqual(1, len(test2_classes))
            self.assertEqual("abc/1", test_classes.value.get("abc/1").Id)
            self.assertEqual("xyz/1", test2_classes[0].Id)
