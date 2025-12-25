from ravendb.documents.smuggler.common import DatabaseItemType

from ravendb.infrastructure.operations import CreateSampleDataOperation

from ravendb.documents.indexes.abstract_index_creation_tasks import AbstractIndexCreationTask
from ravendb.documents.queries.spatial import PointField
from ravendb.tests.test_base import TestBase, Order


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

    def test_can_use_dynamic_query_order_by_spatial_with_alias(self):
        self.store.maintenance.send(CreateSampleDataOperation({DatabaseItemType.DOCUMENTS, DatabaseItemType.INDEXES}))

        with self.store.open_session() as session:
            d = session.advanced.raw_query(
                "from Orders  as a\n" +
                "order by spatial.distance(\n" +
                "    spatial.point(a.ShipTo.Location.Latitude, a.ShipTo.Location.Longitude),\n" +
                "    spatial.point(35.2, -107.2 )\n" +
                ")\n",
                object_type=Order
            ).first()

            metadata = session.advanced.get_metadata_for(d)
            spatial = metadata["@spatial"]

            self.assertAlmostEqual(spatial["Distance"], 48.99, 2)

    def test_can_get_distance_from_spatial_query(self):
        self.store.maintenance.send(CreateSampleDataOperation({DatabaseItemType.DOCUMENTS, DatabaseItemType.INDEXES}))
        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            d = session.query_index("Orders/ByShipment/Location", object_type=Order) \
                .where_equals("id()", "orders/830-A") \
                .order_by_distance("ShipmentLocation", 35.2, -107.1) \
                .single()

            metadata = session.advanced.get_metadata_for(d)
            spatial = metadata["@spatial"]

            self.assertAlmostEqual(spatial["Distance"], 40.1, 1)
