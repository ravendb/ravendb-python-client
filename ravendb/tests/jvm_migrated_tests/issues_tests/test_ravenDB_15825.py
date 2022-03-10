import random
from typing import Optional, List, Dict, Callable

from ravendb.documents.indexes.index_creation import AbstractIndexCreationTask
from ravendb.documents.queries.facets.misc import FacetResult, FacetOptions
from ravendb.documents.queries.index_query import Parameters
from ravendb.documents.queries.utils import HashCalculator
from ravendb.documents.session.document_session import DocumentSession
from ravendb.documents.session.misc import OrderingType
from ravendb.documents.session.query import QueryStatistics
from ravendb.tests.test_base import TestBase


class ContactsIndex(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = "from contact in docs.contacts select new { company_id = contact.company_id, tags = contact.tags, active = contact.active  }"


class Result:
    def __init__(self):
        self.company_id: Optional[int] = None
        self.active: Optional[bool] = None
        self.tags: Optional[List[str]] = None


class Contact:
    def __init__(self, key: str = None, company_id: int = None, active: bool = None, tags: List[str] = None):
        self.key = key
        self.company_id = company_id
        self.active = active
        self.tags = tags


def _facet(
    session: DocumentSession, skip: int, take: int, stats_callback: Callable[[QueryStatistics], None]
) -> Dict[str, FacetResult]:
    facet_options = FacetOptions()
    facet_options.start = skip
    facet_options.page_size = take

    result = (
        session.query_index_type(ContactsIndex, Result)
        .statistics(stats_callback)
        .order_by("company_id", OrderingType.ALPHA_NUMERIC)
        .where_equals("active", True)
        .where_equals("tags", "apple")
        .aggregate_by(lambda b: b.by_field("company_id").with_options(facet_options))
        .execute()
    )

    return result


_TAGS = ["test", "label", "vip", "apple", "orange"]


class TestRavenDB_15285(TestBase):
    def test_should_work(self):
        ContactsIndex().execute(self.store)

        with self.store.open_session() as session:
            for id_ in range(10000):
                company_id = id_ % 100

                contact = Contact()
                contact.key = f"contacts/{id_}"
                contact.company_id = company_id
                contact.active = id_ % 2 == 0
                contact.tags = [_TAGS[id_ % len(_TAGS)]]

                session.store(contact)

            session.save_changes()

        self.wait_for_indexing(self.store)

        stats: Optional[QueryStatistics] = None

        def __stats_callback(query_stats: QueryStatistics):
            nonlocal stats
            stats = query_stats

        with self.store.open_session() as session:
            res = _facet(session, 1, 3, __stats_callback)
            self.assertNotEqual(-1, stats.duration_in_ms)
            self.assertEqual(3, len(res.get("company_id").values))

            self.assertEqual("28", res.get("company_id").values[0].range_)
            self.assertEqual("38", res.get("company_id").values[1].range_)
            self.assertEqual("48", res.get("company_id").values[2].range_)

            stats2: Optional[QueryStatistics] = None

            def __stats2_callback(query_stats: QueryStatistics):
                nonlocal stats2
                stats2 = query_stats

            res2 = _facet(session, 2, 1, __stats2_callback)
            self.assertNotEqual(-1, stats2.duration_in_ms)
            self.assertEqual(1, len(res2.get("company_id").values))
            self.assertEqual("38", res2.get("company_id").values[0].range_)

            stats3: Optional[QueryStatistics] = None

            def __stats3_callback(query_stats: QueryStatistics):
                nonlocal stats3
                stats3 = query_stats

            res3 = _facet(session, 5, 5, __stats3_callback)
            self.assertNotEqual(-1, stats3.duration_in_ms)

            self.assertEqual(5, len(res3.get("company_id").values))
            self.assertEqual("68", res3.get("company_id").values[0].range_)
            self.assertEqual("78", res3.get("company_id").values[1].range_)
            self.assertEqual("8", res3.get("company_id").values[2].range_)
            self.assertEqual("88", res3.get("company_id").values[3].range_)
            self.assertEqual("98", res3.get("company_id").values[4].range_)

    def test_can_hash_correctly(self):
        facet_options = FacetOptions()
        facet_options.start = 1
        facet_options.page_size = 5

        p = Parameters()
        p["p1"] = facet_options

        hash_calculator = HashCalculator()
        hash_calculator.write_parameters(p)
        hash1 = hash_calculator.hash

        facet2_options = FacetOptions()
        facet2_options.start = 1
        facet2_options.page_size = 5

        p2 = Parameters()
        p2["p1"] = facet2_options

        hash_calculator2 = HashCalculator()
        hash_calculator2.write_parameters(p2)
        hash2 = hash_calculator2.hash

        facet_options.start = 2
        hash_calculator3 = HashCalculator()
        hash_calculator3.write_parameters(p)
        hash_3 = hash_calculator3.hash

        self.assertEqual(hash2, hash1)
        self.assertNotEqual(hash_3, hash1)
