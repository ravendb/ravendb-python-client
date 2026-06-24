"""
Regression test for the 7.2.3 spatial-quoting fix.

Before the fix, dynamic spatial fields were wrapped in single quotes in the
order_by_distance* methods (e.g. 'point(...)') which produced invalid RQL
for projection / aliased queries. The fix drops the quotes; the field name
is used verbatim.
"""

import unittest
from typing import Optional

from ravendb.documents.queries.spatial import PointField, WktField
from ravendb.tests.test_base import TestBase


class _Geo:
    def __init__(
        self,
        Id: Optional[str] = None,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
    ):
        self.Id = Id
        self.lat = lat
        self.lng = lng


class TestOrderByDistanceQuoting(TestBase):
    def setUp(self):
        super().setUp()
        with self.store.open_session() as s:
            s.store(_Geo(lat=51.4779, lng=0.0015), "geo/1")  # near Greenwich
            s.store(_Geo(lat=40.7128, lng=-74.0060), "geo/2")  # New York
            s.save_changes()

    def test_order_by_distance_with_dynamic_point_field(self):
        with self.store.open_session() as s:
            results = list(s.query(object_type=_Geo).order_by_distance(PointField("lat", "lng"), 51.4779, 0.0015))
            self.assertGreaterEqual(len(results), 2)
            self.assertEqual("geo/1", results[0].Id)

    def test_order_by_distance_descending_with_dynamic_point_field(self):
        with self.store.open_session() as s:
            results = list(
                s.query(object_type=_Geo).order_by_distance_descending(PointField("lat", "lng"), 51.4779, 0.0015)
            )
            self.assertGreaterEqual(len(results), 2)
            self.assertEqual("geo/2", results[0].Id)

    def test_order_by_distance_wkt_ascending_with_dynamic_point_field(self):
        # Regression for the create_distance_ascending_wkt RQL bug: a stray ')' produced
        # 'spatial.distance(<field>), spatial.wkt(...)', which the server rejects at parse time.
        # WKT is "POINT(longitude latitude)"; the point below is geo/1 (Greenwich).
        with self.store.open_session() as s:
            results = list(
                s.query(object_type=_Geo).order_by_distance_wkt(PointField("lat", "lng"), "POINT(0.0015 51.4779)")
            )
            self.assertGreaterEqual(len(results), 2)
            self.assertEqual("geo/1", results[0].Id)

    def test_order_by_distance_wkt_descending_with_dynamic_point_field(self):
        # Same WKT path, descending: the farthest document (New York) comes first.
        with self.store.open_session() as s:
            results = list(
                s.query(object_type=_Geo).order_by_distance_descending_wkt(
                    PointField("lat", "lng"), "POINT(0.0015 51.4779)"
                )
            )
            self.assertGreaterEqual(len(results), 2)
            self.assertEqual("geo/2", results[0].Id)


if __name__ == "__main__":
    unittest.main()
