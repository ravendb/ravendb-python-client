from ravendb import AbstractIndexCreationTask
from ravendb.documents.indexes.spatial.configuration import SpatialOptions, SpatialSearchStrategy, SpatialRelation
from ravendb.tests.test_base import TestBase


class GeoDocument:
    def __init__(self, wkt: str = None):
        self.wkt = wkt

    def to_json(self):
        return {"WKT": self.wkt}


class GeoIndex(AbstractIndexCreationTask):
    def __init__(self):
        super(GeoIndex, self).__init__()
        self.map = "docs.GeoDocuments.Select(doc => new {\n" + "    WKT = this.CreateSpatialField(doc.WKT)\n" + "})"
        spatial_options = SpatialOptions(strategy=SpatialSearchStrategy.GEOHASH_PREFIX_TREE)
        self._spatial_options_strings["WKT"] = spatial_options


class TestSimonBartlett(TestBase):
    def setUp(self):
        super(TestSimonBartlett, self).setUp()

    def test_line_strings_should_intersect(self):
        self.store.execute_index(GeoIndex())

        with self.store.open_session() as session:
            geo_document = GeoDocument("LINESTRING (0 0, 1 1, 2 1)")
            session.store(geo_document)
            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            count = (
                session.query_index_type(GeoIndex, GeoDocument)
                .spatial("WKT", lambda f: f.relates_to_shape("LINESTRING (1 0, 1 1, 1 2)", SpatialRelation.INTERSECTS))
                .wait_for_non_stale_results()
                .count()
            )

            self.assertEqual(1, count)

            count = (
                session.query_index_type(GeoIndex, object)
                .relates_to_shape("WKT", "LINESTRING (1 0, 1 1, 1 2)", SpatialRelation.INTERSECTS)
                .wait_for_non_stale_results()
                .count()
            )

            self.assertEqual(1, count)

    def test_circles_should_not_intersect(self):
        self.store.execute_index(GeoIndex())

        with self.store.open_session() as session:
            # 110km is approximately 1 degree
            geo_document = GeoDocument("CIRCLE(0.000000 0.000000 d=110)")
            session.store(geo_document)
            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            # should not intersect, as there is 1 Degree between two shapes
            count = (
                session.query_index_type(GeoIndex, GeoDocument)
                .spatial(
                    "WKT", lambda f: f.relates_to_shape("CIRCLE(0.000000 3.000000 d=110)", SpatialRelation.INTERSECTS)
                )
                .wait_for_non_stale_results()
                .count()
            )

            self.assertEqual(0, count)

            count = (
                session.query_index_type(GeoIndex, object)
                .relates_to_shape("WKT", "CIRCLE(0.000000 3.000000 d=110)", SpatialRelation.INTERSECTS)
                .wait_for_non_stale_results()
                .count()
            )

            self.assertEqual(0, count)
