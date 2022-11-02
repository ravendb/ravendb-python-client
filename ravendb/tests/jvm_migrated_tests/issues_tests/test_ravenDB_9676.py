from ravendb import PointField
from ravendb.tests.test_base import TestBase


class Item:
    def __init__(self, name: str = None, latitude: float = None, longitude: float = None):
        self.name = name
        self.latitude = latitude
        self.longitude = longitude


class TestRavenDB9676(TestBase):
    def setUp(self):
        super(TestRavenDB9676, self).setUp()

    def test_can_order_by_distance_on_dynamic_spatial_field(self):
        with self.store.open_session() as session:
            item = Item("Item1", 10.0, 10.0)
            session.store(item)

            item1 = Item("Item2", 11.0, 11.0)
            session.store(item1)

            session.save_changes()

        with self.store.open_session() as session:
            items = list(
                session.query(object_type=Item)
                .wait_for_non_stale_results()
                .spatial(PointField("latitude", "longitude"), lambda f: f.within_radius(1000, 10, 10))
                .order_by_distance(PointField("latitude", "longitude"), 10, 10)
            )
            self.assertEqual(2, len(items))
            self.assertEqual("Item1", items[0].name)
            self.assertEqual("Item2", items[1].name)

            items = list(
                session.query(object_type=Item)
                .wait_for_non_stale_results()
                .spatial(PointField("latitude", "longitude"), lambda f: f.within_radius(1000, 10, 10))
                .order_by_distance_descending(PointField("latitude", "longitude"), 10, 10)
            )
            self.assertEqual(2, len(items))
            self.assertEqual("Item2", items[0].name)
            self.assertEqual("Item1", items[1].name)
