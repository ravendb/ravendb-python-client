from ravendb import PointField
from ravendb.documents.queries.spatial import WktField
from ravendb.tests.test_base import TestBase


class Item:
    def __init__(
        self,
        Id: str = None,
        name: str = None,
        latitude: int = None,
        longitude: int = None,
        latitude2: int = None,
        longitude2: int = None,
        shape_wkt: str = None,
    ):
        self.Id = Id
        self.name = name
        self.latitude = latitude
        self.longitude = longitude
        self.latitude2 = latitude2
        self.longitude2 = longitude2
        self.shape_wkt = shape_wkt


class TestRavenDB8328(TestBase):
    def setUp(self):
        super(TestRavenDB8328, self).setUp()

    def test_spatial_on_auto_index(self):
        with self.store.open_session() as session:
            item = Item(latitude=10, longitude=20, latitude2=10, longitude2=20, shape_wkt="POINT(20 10)", name="Name1")
            session.store(item)
            session.save_changes()

        with self.store.open_session() as session:
            q = session.query(object_type=Item).spatial(
                PointField("latitude", "longitude"), lambda f: f.within_radius(10, 10, 20)
            )
            iq = q.index_query
            self.assertEqual(
                "from 'Items' where spatial.within(spatial.point(latitude, longitude), spatial.circle($p0, $p1, $p2))",
                iq.query,
            )

            q = session.query(object_type=Item).spatial(WktField("shape_wkt"), lambda f: f.within_radius(10, 10, 20))

            iq = q.index_query
            self.assertEqual(
                "from 'Items' where spatial.within(spatial.wkt(shape_wkt), spatial.circle($p0, $p1, $p2))", iq.query
            )
