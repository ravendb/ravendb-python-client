from __future__ import annotations
import datetime
from typing import Dict, Optional

from ravendb import AbstractIndexCreationTask, QueryStatistics
from ravendb.documents.indexes.definitions import FieldIndexing
from ravendb.tests.test_base import TestBase


class SpatialIdx(AbstractIndexCreationTask):
    def __init__(self):
        super(SpatialIdx, self).__init__()
        self.map = (
            "docs.Events.Select(e => new {\n"
            "    capacity = e.capacity,\n"
            "    venue = e.venue,\n"
            "    date = e.date,\n"
            "    coordinates = this.CreateSpatialField(((double ? ) e.latitude), ((double ? ) e.longitude))\n"
            "})"
        )
        self._index("venue", FieldIndexing.SEARCH)


class Event:
    def __init__(
        self,
        venue: str = None,
        latitude: float = None,
        longitude: float = None,
        date: datetime.datetime = None,
        capacity: int = None,
    ):
        self.venue = venue
        self.latitude = latitude
        self.longitude = longitude
        self.date = date
        self.capacity = capacity

    @classmethod
    def from_json(cls, json_dict: Dict) -> Event:
        return cls(
            json_dict["venue"],
            json_dict["latitude"],
            json_dict["longitude"],
            datetime.datetime.fromisoformat(json_dict["date"]) if json_dict["date"] else None,
            json_dict["capacity"],
        )

    def to_json(self) -> Dict:
        return {
            "venue": self.venue,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "date": self.date.isoformat() if self.date else None,
            "capacity": self.capacity,
        }


class TestSpatialSearch(TestBase):
    def setUp(self):
        super(TestSpatialSearch, self).setUp()

    def test_can_do_spatial_search_with_client_api_3(self):
        SpatialIdx().execute(self.store)
        with self.store.open_session() as session:
            matching_venues = (
                session.advanced.document_query_from_index_type(SpatialIdx, Event)
                .spatial("coordinates", lambda f: f.within_radius(5, 38.9103000, -77.3942))
                .wait_for_non_stale_results()
            )
            iq = matching_venues.index_query
            self.assertEqual(
                "from index 'SpatialIdx' where spatial.within(coordinates, spatial.circle($p0, $p1, $p2))", iq.query
            )

            self.assertEqual(5.0, iq.query_parameters.get("p0"))
            self.assertEqual(38.9103, iq.query_parameters.get("p1"))
            self.assertEqual(-77.3942, iq.query_parameters.get("p2"))

    def test_can_do_spatial_search_with_client_api_within_given_capacity(self):
        SpatialIdx().execute(self.store)
        now = datetime.datetime.now()

        with self.store.open_session() as session:
            session.store(Event("a/1", 38.9579000, -77.3572000, now, 5000))
            session.store(Event("a/2", 38.9690000, -77.3862000, now + datetime.timedelta(days=1), 5000))
            session.store(Event("b/2", 38.9690000, -77.3862000, now + datetime.timedelta(days=2), 2000))
            session.store(Event("c/3", 38.9510000, -77.4107000, now + datetime.timedelta(days=365 * 3), 1500))
            session.store(Event("d/1", 37.9510000, -77.4107000, now + datetime.timedelta(days=365 * 3), 1500))
            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            query_stats: Optional[QueryStatistics] = None

            def _stats_callback(stats: QueryStatistics):
                nonlocal query_stats
                query_stats = stats

            events = list(
                session.query_index("SpatialIdx", Event)
                .statistics(_stats_callback)
                .open_subclause()
                .where_greater_than_or_equal("capacity", 0)
                .and_also()
                .where_less_than_or_equal("capacity", 2000)
                .close_subclause()
                .within_radius_of("coordinates", 6.0, 38.96939, -77.386398)
                .order_by_descending("date")
            )

            self.assertEqual(2, query_stats.total_results)
            self.assertEqual(["c/3", "b/2"], list(map(lambda x: x.venue, events)))

    def test_can_do_spatial_search_with_client_api_add_order(self):
        SpatialIdx().execute(self.store)
        with self.store.open_session() as session:
            session.store(Event("a/1", 38.9579000, -77.3572000))
            session.store(Event("b/1", 38.9579000, -77.3572000))
            session.store(Event("c/1", 38.9579000, -77.3572000))
            session.store(Event("a/2", 38.969000, -77.3862000))
            session.store(Event("b/2", 38.9690000, -77.3862000))
            session.store(Event("c/2", 38.9690000, -77.3862000))
            session.store(Event("a/3", 38.9510000, -77.4107000))
            session.store(Event("b/3", 38.9510000, -77.4107000))
            session.store(Event("c/3", 38.9510000, -77.4107000))
            session.store(Event("d/1", 37.9510000, -77.4107000))
            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            events = list(
                session.query_index("spatialIdx", Event)
                .within_radius_of("coordinates", 6.0, 38.96939, -77.386398)
                .order_by_distance("coordinates", 38.96939, -77.386398)
                .add_order("venue", False)
            )

            self.assertEqual(
                ["a/2", "b/2", "c/2", "a/1", "b/1", "c/1", "a/3", "b/3", "c/3"], list(map(lambda x: x.venue, events))
            )

        with self.store.open_session() as session:
            events = list(
                session.query_index("spatialIdx", Event)
                .within_radius_of("coordinates", 6.0, 38.96939, -77.386398)
                .add_order("venue", False)
                .order_by_distance("coordinates", 38.96939, -77.386398)
            )

            self.assertEqual(
                ["a/1", "a/2", "a/3", "b/1", "b/2", "b/3", "c/1", "c/2", "c/3"], list(map(lambda x: x.venue, events))
            )

    def test_can_do_spatial_search_with_client_api(self):
        SpatialIdx().execute(self.store)
        now = datetime.datetime.now()

        with self.store.open_session() as session:
            session.store(Event("a/1", 38.9579000, -77.3572000, now, 5000))
            session.store(Event("a/2", 38.9690000, -77.3862000, now + datetime.timedelta(days=1), 5000))
            session.store(Event("b/2", 38.9690000, -77.3862000, now + datetime.timedelta(days=2), 2000))
            session.store(Event("c/3", 38.9510000, -77.4107000, now + datetime.timedelta(days=365 * 3), 1500))
            session.store(Event("d/1", 37.9510000, -77.4107000, now + datetime.timedelta(days=365 * 3), 1500))
            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            query_stats: Optional[QueryStatistics] = None

            def _stats_callback(stats: QueryStatistics):
                nonlocal query_stats
                query_stats = stats

            events = list(
                session.query_index("SpatialIdx", Event)
                .statistics(_stats_callback)
                .where_less_than_or_equal("date", now + datetime.timedelta(days=365))
                .within_radius_of("coordinates", 6.0, 38.96939, -77.386398)
                .order_by_descending("date")
            )
            self.assertTrue(events)
