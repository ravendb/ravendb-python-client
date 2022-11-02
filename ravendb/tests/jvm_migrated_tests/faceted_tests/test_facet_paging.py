import datetime
import itertools
import random
from typing import List

from ravendb import Facet, DocumentStore, IndexDefinition, PutIndexesOperation
from ravendb.documents.queries.facets.definitions import FacetSetup
from ravendb.documents.queries.facets.misc import FacetOptions, FacetTermSortMode
from ravendb.tests.test_base import TestBase


class Camera:
    def __init__(
        self,
        Id: str = None,
        date_of_listing: datetime.datetime = None,
        manufacturer: str = None,
        model: str = None,
        cost: float = None,
        zoom: int = None,
        megapixels: float = None,
        image_stabilizer: bool = None,
        advanced_features: List[str] = None,
    ):
        self.Id = Id
        self.date_of_listing = date_of_listing
        self.manufacturer = manufacturer
        self.model = model
        self.cost = cost
        self.zoom = zoom
        self.megapixels = megapixels
        self.image_stabilizer = image_stabilizer
        self.advanced_features = advanced_features


FEATURES = ["Image Stabilizer", "Tripod", "Low Light Compatible", "Fixed Lens", "LCD"]
MANUFACTURERS = ["Sony", "Nikon", "Phillips", "Canon", "Jessops"]
MODELS = ["Model1", "Model2", "Model3", "Model4", "Model5"]


class TestFacetPaging(TestBase):
    def _get_cameras(self, num_cameras: int) -> List[Camera]:
        camera_list = []
        for i in range(1, num_cameras):
            camera = Camera(
                date_of_listing=datetime.datetime(
                    80 + random.randint(1, 30), random.randint(1, 12), random.randint(1, 27)
                ),
                manufacturer=MANUFACTURERS[random.randint(0, len(MANUFACTURERS) - 1)],
                model=MODELS[random.randint(0, len(MODELS) - 1)],
                cost=random.randint(0, 100) * 9 + 100,
                zoom=random.randint(0, 10) + 1,
                megapixels=random.randint(0, 10) + 1,
                image_stabilizer=random.randint(0, 100) > 60,
                advanced_features=["??"],
            )
            camera_list.append(camera)
        return camera_list

    def setUp(self):
        super(TestFacetPaging, self).setUp()
        self._num_cameras = 1000
        self._data = self._get_cameras(self._num_cameras)

    def _setup(self, store: DocumentStore):
        with store.open_session() as session:
            index_definition = IndexDefinition()
            index_definition.name = "CameraCost"
            index_definition.maps = [
                "from camera in docs select new { camera.manufacturer, camera.model, camera.cost, camera.date_of_listing, camera.megapixels } "
            ]
            store.maintenance.send(PutIndexesOperation(index_definition))

            counter = 0
            for camera in self._data:
                session.store(camera)
                counter += 1

                if counter % (self._num_cameras / 25) == 0:
                    session.save_changes()

            session.save_changes()

        self.wait_for_indexing(store)

    def test_can_perform_faceted_paging_search_with_no_page_size_no_max_results_hits_desc(self):
        facet_options = FacetOptions()
        facet_options.start = 2
        facet_options.term_sort_mode = FacetTermSortMode.COUNT_DESC
        facet_options.include_remaining_terms = True

        facet = Facet(field_name="manufacturer")
        facet.options = facet_options

        facets = [facet]
        self._setup(self.store)

        with self.store.open_session() as session:
            facet_setup = FacetSetup(facets)
            session.store(facet_setup, "facets/CameraFacets")
            session.save_changes()

            facet_results = session.query_index("CameraCost", Camera).aggregate_using("facets/CameraFacets").execute()

            # todo: itertools group by
            camera_counts = {manufacturer: 0 for manufacturer in MANUFACTURERS}
            for d in self._data:
                camera_counts[d.manufacturer] += 1

            cameras_by_hits = list(
                map(lambda x: x[0].lower(), sorted(camera_counts.items(), key=lambda item:(-item[1],item[0]))[2:])
            )

            self.assertEqual(3, len(facet_results["manufacturer"].values))
            self.assertEqual(cameras_by_hits[0], facet_results["manufacturer"].values[0].range_)
            self.assertEqual(cameras_by_hits[1], facet_results["manufacturer"].values[1].range_)
            self.assertEqual(cameras_by_hits[2], facet_results["manufacturer"].values[2].range_)

            for f in facet_results["manufacturer"].values:
                in_memory_count = len(list(filter(lambda x: x.manufacturer.lower() == f.range_.lower(), self._data)))
                self.assertEqual(in_memory_count, f.count_)
                self.assertEqual(0, facet_results["manufacturer"].remaining_terms_count)
                self.assertEqual(0, len(facet_results["manufacturer"].remaining_terms))
                self.assertEqual(0, facet_results["manufacturer"].remaining_hits)

    def test_can_perform_faceted_paging_search_with_no_page_size_with_max_results_hits_desc(self):
        facet_options = FacetOptions()
        facet_options.start = 2
        facet_options.page_size = 2
        facet_options.term_sort_mode = FacetTermSortMode.COUNT_DESC
        facet_options.include_remaining_terms = True

        facet = Facet(field_name="manufacturer")
        facet.options = facet_options

        facets = [facet]
        self._setup(self.store)

        with self.store.open_session() as session:
            facet_setup = FacetSetup(facets)
            session.store(facet_setup, "facets/CameraFacets")
            session.save_changes()

            facet_results = session.query_index("CameraCost", Camera).aggregate_using("facets/CameraFacets").execute()
            camera_counts = {manufacturer: 0 for manufacturer in MANUFACTURERS}
            for d in self._data:
                camera_counts[d.manufacturer] += 1

            cameras_by_hits = list(
                map(lambda x: x[0].lower(), sorted(camera_counts.items(), key=lambda item:(-item[1],item[0]))[2:])
            )

            self.assertEqual(2, len(facet_results.get("manufacturer").values))
            self.assertEqual(cameras_by_hits[0], facet_results.get("manufacturer").values[0].range_)
            self.assertEqual(cameras_by_hits[1], facet_results.get("manufacturer").values[1].range_)

            for f in facet_results.get("manufacturer").values:
                in_memory_count = len(list(filter(lambda x: x.manufacturer.lower() == f.range_.lower(), self._data)))
                self.assertEqual(in_memory_count, f.count_)
                self.assertEqual(1, facet_results.get("manufacturer").remaining_terms_count)
                self.assertEqual(1, len(facet_results.get("manufacturer").remaining_terms))
                counts = list(
                    map(lambda x: x[1], sorted(camera_counts.items(), key=lambda item: (-item[1], item[0]))[2:])
                )
                self.assertEqual(facet_results.get("manufacturer").remaining_hits, counts[-1])
