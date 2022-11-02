from typing import List

from ravendb import DocumentStore, IndexDefinition, IndexFieldOptions, PutIndexesOperation
from ravendb.documents.indexes.definitions import FieldIndexing
from ravendb.tests.test_base import TestBase

FILTERED_LAT = 44.419575
FILTERED_LNG = 34.042618
SORTED_LAT = 44.417398
SORTED_LNG = 34.042575
FILTERED_RADIUS = 100


class Shop:
    def __init__(self, Id: str = None, latitude: float = None, longitude: float = None):
        self.Id = Id
        self.latitude = latitude
        self.longitude = longitude


shops = [
    Shop(latitude=44.420678, longitude=34.042490),
    Shop(latitude=44.419712, longitude=34.042232),
    Shop(latitude=44.418686, longitude=34.043219),
]

sorted_expected_order = ["shops/3-A", "shops/2-A", "shops/1-A"]
filtered_expected_order = ["shops/2-A", "shops/3-A", "shops/1-A"]


def get_query_shape_from_lat_lon(lat: float, lng: float, radius: float):
    return f"Circle({lng} {lat} d={radius})"


class TestSpatialSorting(TestBase):
    def setUp(self):
        super(TestSpatialSorting, self).setUp()

    def create_data(self, store: DocumentStore):
        index_definition = IndexDefinition()
        index_definition.name = "eventsByLatLng"
        index_definition.maps = [
            "from e in docs.Shops select new { e.venue, coordinates = CreateSpatialField(e.latitude, e.longitude) }"
        ]

        fields = dict()
        options = IndexFieldOptions(indexing=FieldIndexing.EXACT)
        fields["tag"] = options
        index_definition.fields = fields

        store.maintenance.send(PutIndexesOperation(index_definition))

        index_definition_2 = IndexDefinition()
        index_definition_2.name = "eventsByLatLngWSpecialField"
        index_definition_2.maps = [
            "from e in docs.Shops select new { e.venue, mySpacialField = CreateSpatialField(e.latitude, e.longitude) }"
        ]

        index_field_options = IndexFieldOptions(indexing=FieldIndexing.EXACT)
        index_definition_2.fields = {"tag": index_field_options}

        store.maintenance.send(PutIndexesOperation(index_definition_2))

        with self.store.open_session() as session:
            for shop in shops:
                session.store(shop)
            session.save_changes()

        self.wait_for_indexing(self.store)

    def __assert_results_order(self, result_ids: List[str] = None, expected_order: List[str] = None):
        self.assertEqual(expected_order, result_ids)

    def test_can_sort_by_distance_w_o_filtering(self):
        self.create_data(self.store)

        with self.store.open_session() as session:
            shops = session.query_index("eventsByLatLng", Shop).order_by_distance(
                "coordinates", FILTERED_LAT, FILTERED_LNG
            )
            self.__assert_results_order(list(map(lambda x: x.Id, shops)), filtered_expected_order)

            with self.store.open_session() as session:
                shops = list(
                    session.query_index("eventsByLatLng", Shop).order_by_distance_descending(
                        "coordinates", FILTERED_LAT, FILTERED_LNG
                    )
                )
                strings = list(map(lambda x: x.Id, shops))
                strings.reverse()
                self.__assert_results_order(strings, filtered_expected_order)

    def test_can_sort_by_distance_w_o_filtering_by_specified_field(self):
        self.create_data(self.store)

        with self.store.open_session() as session:
            shops = list(
                session.query_index("eventsByLatLngWSpecialField", Shop).order_by_distance(
                    "mySpacialField", FILTERED_LAT, FILTERED_LNG
                )
            )
            self.__assert_results_order(list(map(lambda x: x.Id, shops)), filtered_expected_order)

        with self.store.open_session() as session:
            shops = list(
                session.query_index("eventsByLatLngWSpecialField", Shop).order_by_distance_descending(
                    "mySpacialField", FILTERED_LAT, FILTERED_LNG
                )
            )
            strings = list(map(lambda x: x.Id, shops))
            strings.reverse()
            self.__assert_results_order(strings, filtered_expected_order)

    def test_can_filter_by_location_and_sort_by_distance_from_different_point_w_doc_query(self):
        self.create_data(self.store)

        with self.store.open_session() as session:
            shops = list(
                session.query_index("eventsByLatLng", Shop)
                .spatial(
                    "coordinates",
                    lambda f: f.within(get_query_shape_from_lat_lon(FILTERED_LAT, FILTERED_LNG, FILTERED_RADIUS)),
                )
                .order_by_distance("coordinates", SORTED_LAT, SORTED_LNG)
            )
            self.__assert_results_order(list(map(lambda x: x.Id, shops)), sorted_expected_order)

    def test_can_sort_by_distance_w_o_filtered_w_doc_query_by_specified_field(self):
        self.create_data(self.store)

        with self.store.open_session() as session:
            shops = list(
                session.query_index("eventsByLatLngWSpecialField", Shop).order_by_distance(
                    "mySpacialField", SORTED_LAT, SORTED_LNG
                )
            )
            self.__assert_results_order(list(map(lambda x: x.Id, shops)), sorted_expected_order)

    def test_can_sort_by_distance_w_o_filtering_w_doc_query(self):
        self.create_data(self.store)

        with self.store.open_session() as session:
            shops = list(
                session.query_index("eventsByLatLng", Shop).order_by_distance("coordinates", SORTED_LAT, SORTED_LNG)
            )
            self.__assert_results_order(list(map(lambda x: x.Id, shops)), sorted_expected_order)

    def test_can_sort_by_distance_w_o_filtering_w_doc_query_by_specified_field(self):
        self.create_data(self.store)

        with self.store.open_session() as session:
            shops = list(
                session.query_index("eventsByLatLngWSpecialField", Shop).order_by_distance(
                    "mySpacialField", SORTED_LAT, SORTED_LNG
                )
            )
            self.__assert_results_order(list(map(lambda x: x.Id, shops)), sorted_expected_order)
