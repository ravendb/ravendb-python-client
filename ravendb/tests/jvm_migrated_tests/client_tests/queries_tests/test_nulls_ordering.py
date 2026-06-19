from typing import Optional

from ravendb import NullsOrdering, OrderingType
from ravendb.exceptions.raven_exceptions import RavenException
from ravendb.tests.test_base import TestBase


class OrderingDoc:
    def __init__(self, name: Optional[str] = None, age: Optional[int] = None):
        self.name = name
        self.age = age


class TestNullsOrdering(TestBase):
    def setUp(self):
        super().setUp()

    # ---- RQL generation (client-side; engine independent) ----

    def test_order_by_renders_nulls_first(self):
        with self.store.open_session() as session:
            query = session.advanced.document_query(object_type=OrderingDoc).order_by("name", nulls=NullsOrdering.FIRST)
            self.assertEqual("from 'OrderingDocs' order by name nulls first", query._to_string())

    def test_order_by_with_ordering_type_renders_nulls_last(self):
        with self.store.open_session() as session:
            query = session.advanced.document_query(object_type=OrderingDoc).order_by(
                "age", OrderingType.LONG, NullsOrdering.LAST
            )
            self.assertEqual("from 'OrderingDocs' order by age as long nulls last", query._to_string())

    def test_order_by_descending_renders_desc_then_nulls(self):
        with self.store.open_session() as session:
            query = session.advanced.document_query(object_type=OrderingDoc).order_by_descending(
                "name", nulls=NullsOrdering.FIRST
            )
            self.assertEqual("from 'OrderingDocs' order by name desc nulls first", query._to_string())

    def test_default_nulls_ordering_emits_no_clause(self):
        with self.store.open_session() as session:
            query = session.advanced.document_query(object_type=OrderingDoc).order_by("name")
            self.assertEqual("from 'OrderingDocs' order by name", query._to_string())

    def test_add_order_threads_nulls(self):
        with self.store.open_session() as session:
            query = session.advanced.document_query(object_type=OrderingDoc).add_order(
                "age", True, OrderingType.LONG, NullsOrdering.LAST
            )
            self.assertEqual("from 'OrderingDocs' order by age as long desc nulls last", query._to_string())

    def test_order_by_distance_renders_nulls(self):
        with self.store.open_session() as session:
            query = session.advanced.document_query(object_type=OrderingDoc).order_by_distance(
                "loc", 10.0, 20.0, nulls=NullsOrdering.FIRST
            )
            self.assertEqual(
                "from 'OrderingDocs' order by spatial.distance(loc, spatial.point($p0, $p1)) nulls first",
                query._to_string(),
            )

    # ---- End-to-end placement (requires Corax, the default auto-index engine in 7.x) ----

    def test_nulls_first_and_last_place_null_values(self):
        # End-to-end placement requires a server (7.2.3+) and a Corax index. Older servers reject the
        # 'nulls first/last' RQL at parse time; self-skip there so the suite stays green.
        with self.store.open_session() as session:
            session.store(OrderingDoc("a", 5), "docs/1")
            session.store(OrderingDoc("b", None), "docs/2")
            session.store(OrderingDoc("c", 3), "docs/3")
            session.save_changes()

        def query(nulls):
            with self.store.open_session() as session:
                return list(
                    session.advanced.document_query(object_type=OrderingDoc)
                    .wait_for_non_stale_results()
                    .order_by("age", OrderingType.LONG, nulls)
                )

        try:
            first = query(NullsOrdering.FIRST)
            last = query(NullsOrdering.LAST)
        except RavenException as e:
            message = str(e).lower()
            if "nulls" in message or "corax" in message:
                self.skipTest(
                    "Server does not support per-query 'nulls first/last' ordering (needs 7.2.3+ with Corax)."
                )
            raise

        self.assertEqual(3, len(first))
        self.assertIsNone(first[0].age)
        self.assertEqual(3, len(last))
        self.assertIsNone(last[-1].age)
