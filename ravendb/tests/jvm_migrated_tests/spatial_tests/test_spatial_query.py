from ravendb import IndexDefinition, PutIndexesOperation
from ravendb.documents.indexes.spatial.configuration import SpatialUnits
from ravendb.tests.test_base import TestBase


class DummyGeoDoc:
    def __init__(self, Id: str = None, latitude: float = None, longitude: float = None):
        self.Id = Id
        self.latitude = latitude
        self.longitude = longitude


class TestSpatialQuery(TestBase):
    def setUp(self):
        super(TestSpatialQuery, self).setUp()

    def test_can_successfully_query_by_miles(self):
        my_house = DummyGeoDoc(latitude=44.757767, longitude=-93.355322)
        # The gym is about 7.32 miles (11.79 kilometers) from my house.
        gym = DummyGeoDoc(latitude=44.682861, longitude=-93.25)

        with self.store.open_session() as session:
            session.store(my_house)
            session.store(gym)
            session.save_changes()

            index_definition = IndexDefinition()
            index_definition.name = "FindByLatLng"
            index_definition.maps = {
                "from doc in docs select new { coordinates = CreateSpatialField(doc.latitude, doc.longitude) }"
            }

            self.store.maintenance.send(PutIndexesOperation(index_definition))

            # Wait until the index is built
            list(session.query_index("FindByLatLng", DummyGeoDoc).wait_for_non_stale_results())

            radius = 8.0

            # Find within 8 miles.
            # We should find both my house and the gym.
            matches_within_miles = list(
                session.query_index("FindByLatLng", DummyGeoDoc)
                .within_radius_of("coordinates", radius, my_house.latitude, my_house.longitude, SpatialUnits.MILES)
                .wait_for_non_stale_results()
            )

            self.assertIsNotNone(matches_within_miles)
            self.assertEqual(2, len(matches_within_miles))

            # Find within 8 kilometers.
            # We should find both my house and the gym.
            matches_within_kilometers = list(
                session.query_index("FindByLatLng", DummyGeoDoc)
                .within_radius_of("coordinates", radius, my_house.latitude, my_house.longitude, SpatialUnits.KILOMETERS)
                .wait_for_non_stale_results()
            )

            self.assertIsNotNone(matches_within_kilometers)
            self.assertEqual(1, len(matches_within_kilometers))

    def test_can_successfully_do_spatial_query_of_nearby_locations(self):
        # These items are in a radius of 4 miles (approx 6,5 km)
        area_one_doc_one = DummyGeoDoc(latitude=55.6880508001, longitude=13.5717346673)
        area_one_doc_two = DummyGeoDoc(latitude=55.6821978456, longitude=13.6076183965)
        area_one_doc_three = DummyGeoDoc(latitude=55.673251569, longitude=13.5946697607)

        # This item is 12 miles (approx 19km) from the closest in area0ne
        close_but_outside_area_one = DummyGeoDoc(latitude=55.8634157297, longitude=13.5497731987)

        # This item is about 3900 miles from area0ne
        new_york = DummyGeoDoc(latitude=40.7137578228, longitude=-74.0126901936)

        with self.store.open_session() as session:
            session.store(area_one_doc_one)
            session.store(area_one_doc_two)
            session.store(area_one_doc_three)
            session.store(close_but_outside_area_one)
            session.store(new_york)
            session.save_changes()

            index_definition = IndexDefinition()
            index_definition.name = "FindByLatLng"
            index_definition.maps = {
                "from doc in docs select new { coordinates = CreateSpatialField(doc.latitude, doc.longitude) }"
            }

            self.store.maintenance.send(PutIndexesOperation(index_definition))

            # Wait until the index is built
            list(session.query_index("FindByLatLng", DummyGeoDoc).wait_for_non_stale_results())

            # in the middle of AreaOne
            lat = 55.6836422426
            lng = 13.5871808352
            radius = 5.0

            nearby_docs = list(
                session.query_index("FindByLatLng", DummyGeoDoc)
                .within_radius_of("coordinates", radius, lat, lng)
                .wait_for_non_stale_results()
            )

            self.assertIsNotNone(nearby_docs)
            self.assertEqual(3, len(nearby_docs))
