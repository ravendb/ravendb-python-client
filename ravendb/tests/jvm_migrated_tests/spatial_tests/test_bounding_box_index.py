from ravendb import AbstractIndexCreationTask
from ravendb.documents.indexes.spatial.configuration import SpatialOptionsFactory
from ravendb.tests.test_base import TestBase


class SpatialDoc:
    def __init__(self, shape: str = None):
        self.shape = shape


class BBoxIndex(AbstractIndexCreationTask):
    def __init__(self):
        super(BBoxIndex, self).__init__()
        self.map = "docs.SpatialDocs.Select(doc => new {\n" "    shape = this.CreateSpatialField(doc.shape)\n" "})"
        self._spatial("shape", lambda x: x.cartesian().bounding_box_index())


class QuadTreeIndex(AbstractIndexCreationTask):
    def __init__(self):
        super(QuadTreeIndex, self).__init__()
        self.map = "docs.SpatialDocs.Select(doc => new {\n" "    shape = this.CreateSpatialField(doc.shape)\n" "})"
        self._spatial(
            "shape",
            lambda x: x.cartesian().quad_prefix_tree_index(6, SpatialOptionsFactory.SpatialBounds(0, 0, 16, 16)),
        )


class TestBoundingBoxIndex(TestBase):
    def setUp(self):
        super(TestBoundingBoxIndex, self).setUp()

    def test_bounding_box(self):
        polygon = "POLYGON ((0 0, 0 5, 1 5, 1 1, 5 1, 5 5, 6 5, 6 0, 0 0))"
        rectangle1 = "2 2 4 4"
        rectangle2 = "6 6 10 10"
        rectangle3 = "0 0 6 6"

        BBoxIndex().execute(self.store)
        QuadTreeIndex().execute(self.store)

        with self.store.open_session() as session:
            doc = SpatialDoc()
            doc.shape = polygon
            session.store(doc)
            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            result = session.query(object_type=SpatialDoc).count()
            self.assertEqual(1, result)

        with self.store.open_session() as session:
            result = (
                session.query_index_type(BBoxIndex, SpatialDoc)
                .spatial("shape", lambda x: x.intersects(rectangle1))
                .count()
            )
            self.assertEqual(1, result)

        with self.store.open_session() as session:
            result = (
                session.query_index_type(BBoxIndex, SpatialDoc)
                .spatial("shape", lambda x: x.intersects(rectangle2))
                .count()
            )
            self.assertEqual(0, result)

        with self.store.open_session() as session:
            result = (
                session.query_index_type(BBoxIndex, SpatialDoc)
                .spatial("shape", lambda x: x.disjoint(rectangle2))
                .count()
            )
            self.assertEqual(1, result)

        with self.store.open_session() as session:
            result = (
                session.query_index_type(BBoxIndex, SpatialDoc).spatial("shape", lambda x: x.within(rectangle3)).count()
            )
            self.assertEqual(1, result)

        with self.store.open_session() as session:
            result = (
                session.query_index_type(QuadTreeIndex, SpatialDoc)
                .spatial("shape", lambda x: x.intersects(rectangle2))
                .count()
            )
            self.assertEqual(0, result)

        with self.store.open_session() as session:
            result = (
                session.query_index_type(QuadTreeIndex, SpatialDoc)
                .spatial("shape", lambda x: x.intersects(rectangle1))
                .count()
            )
            self.assertEqual(0, result)
