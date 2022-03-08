from pyravendb.tests.test_base import TestBase


class Doc:
    def __init__(
        self,
        str_val_1: str = None,
        str_val_2: str = None,
        str_val_3: str = None,
    ):
        self.str_val_1 = str_val_1
        self.str_val_2 = str_val_2
        self.str_val_3 = str_val_3


class TestRavenDB15693(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_query_on_complex_boost(self):
        with self.store.open_session() as session:
            q = (
                session.advanced.document_query(object_type=Doc)
                .search("str_val_1", "a")
                .and_also()
                .open_subclause()
                .search("str_val_2", "b")
                .or_else()
                .search("str_val_3", "search")
                .close_subclause()
                .boost(0.2)
            )

            query_boost = q._to_string()

            self.assertEqual(
                "from 'Docs' where search(str_val_1, $p0) "
                "and boost(search(str_val_2, $p1) "
                "or search(str_val_3, $p2), $p3)",
                query_boost,
            )

            list(q)
