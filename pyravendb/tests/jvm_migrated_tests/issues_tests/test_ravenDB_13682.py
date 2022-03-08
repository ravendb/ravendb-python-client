from pyravendb.documents.indexes.index_creation import AbstractIndexCreationTask
from pyravendb.documents.queries.spatial import PointField
from pyravendb.tests.test_base import TestBase


class Item:
    def __init__(self, lat: float, lng: float, name: str):
        self.lat = lat
        self.lng = lng
        self.name = name


class SpatialIndex(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = (
            "docs.Items.Select(doc => new{\n"
            "    name = doc.name, \n"
            "    coordinates = this.CreateSpatialField(doc.lat, doc.lng)\n"
            "})"
        )


class TestRavenDB(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_query_by_rounded_spatial_ranges(self):
        with self.store.open_session() as session:
            item1 = Item(35.1, -107.1, "a")  # 3rd dist - 72.7 km
            session.store(item1)

            item2 = Item(35.2, -107.0, "b")  # 2nd dist - 64.04 km
            session.store(item2)

            item3 = Item(35.3, -106.5, "c")  # 1st dist - 28.71 km
            session.store(item3)

            session.save_changes()

        with self.store.open_session() as session:
            # we sort first by spatial distance (but round it up to 25km)
            # then we sort by name ascending, so within 25 range, we can apply a different sort

            result = list(
                session.advanced.raw_query(
                    "from Items as a order by spatial.distance(spatial.point(a.lat, a.lng), spatial.point(35.1, -106.3), 25), name",
                    Item,
                )
            )

            self.assertEqual(3, len(result))

            self.assertEqual("c", result[0].name)
            self.assertEqual("a", result[1].name)
            self.assertEqual("b", result[2].name)

        # dynamic query
        with self.store.open_session() as session:
            # we sort first by spatial distance (but round it up to 25km)
            # then we sort by name ascending, so within 25 range, we can apply a different sort

            query = session.query(object_type=Item).order_by_distance(
                PointField("lat", "lng").round_to(25), 35.1, -106.3
            )
            result = list(query)

            self.assertEqual(3, len(result))

            self.assertEqual("c", result[0].name)
            self.assertEqual("a", result[1].name)
            self.assertEqual("b", result[2].name)

        SpatialIndex().execute(self.store)
        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            # we sort first by spatial distance (but round it up to 25km)
            # then we sort by name ascending, so within 25 range, we can apply a different sort

            query = session.query_index_type(SpatialIndex, Item).order_by_distance("coordinates", 35.1, -106.3, 25)
            result = list(query)

            self.assertEqual(3, len(result))

            self.assertEqual("c", result[0].name)
            self.assertEqual("a", result[1].name)
            self.assertEqual("b", result[2].name)
