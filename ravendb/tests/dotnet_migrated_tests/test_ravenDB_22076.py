from typing import List

from ravendb import AbstractIndexCreationTask
from ravendb.documents.indexes.vector.embedding import VectorEmbeddingType
from ravendb.documents.indexes.vector.options import VectorOptions
from ravendb.tests.test_base import TestBase


class Dto:
    def __init__(
        self,
        embedding_base_64: str,
        embedding_singles: List[float],
        embedding_sbytes: List[int],
        embedding_binary: List[int],
    ):
        self.embedding_base_64 = embedding_base_64
        self.embedding_singles = embedding_singles
        self.embedding_sbytes = embedding_sbytes
        self.embedding_binary = embedding_binary


class DummyIndex(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = """
        from dto in docs.Dtos
        select new 
        { 
            Singles = CreateVector(dto.embedding_singles), 
            Integers = CreateVector(dto.embedding_sbytes), 
            Binary = CreateVector(dto.embedding_binary) 
        }
        """
        self._vector("Integers", VectorOptions(VectorEmbeddingType.INT8))
        self._vector("Binary", VectorOptions(VectorEmbeddingType.BINARY))


class IndexWithSetDimensions(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = """
        from dto in docs.Dtos
        select new 
        {
            Singles = CreateVector(dto.embedding_singles) 
        }
        """
        self._vector("Singles", VectorOptions(dimensions=256))


class IndexWithSetDimensionsInt8(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = """
            from dto in docs.Dtos
            select new 
            { 
                Sbytes = CreateVector(dto.embedding_singles) 
            }
            """
        self._vector("Sbytes", VectorOptions(destination_embedding_type=VectorEmbeddingType.INT8, dimensions=22))


class TestRavenDB22076(TestBase):
    def test_rql_generation(self):
        with self.store.open_session() as session:
            q1 = session.query(object_type=Dto).vector_search_i8("EmbeddingField", [2, 3], 0.65, 12)
            self.assertEqual(
                "from 'Dtoes' where vector.search(embedding.i8(EmbeddingField), $p0, 0.65, 12)", q1._to_string()
            )

            # Search by other type (text) than source type (f32)
            q2 = session.query(object_type=Dto).vector_search("VectorField", "aaaa")
            self.assertEqual("from 'Dtoes' where vector.search(VectorField, $p0)", q2._to_string())

            q3 = session.query(object_type=Dto).vector_search("VectorField", [0.3, 0.4, 0.5])
            self.assertEqual("from 'Dtoes' where vector.search(VectorField, $p0)", q3._to_string())

            q4 = session.query(object_type=Dto).vector_search("VectorField", "aaaa==")
            self.assertEqual("from 'Dtoes' where vector.search(VectorField, $p0)", q4._to_string())

            q5 = session.query(object_type=Dto).vector_search_text_i8("TextField", "aaaa")
            self.assertEqual("from 'Dtoes' where vector.search(embedding.text_i8(TextField), $p0)", q5._to_string())

            q6 = session.query(object_type=Dto).vector_search_i8("EmbeddingField", [2, 3], 0.65)
            self.assertEqual(
                "from 'Dtoes' where vector.search(embedding.i8(EmbeddingField), $p0, 0.65, null)", q6._to_string()
            )

            q7 = session.query(object_type=Dto).vector_search_text_i8("TextField", "aaaa")
            self.assertEqual("from 'Dtoes' where vector.search(embedding.text_i8(TextField), $p0)", q7._to_string())

    def test_rql_generation_async(self):
        with self.store.open_session() as session:

            # -- Not applicable for Python - here we just don't have such methods in the API, making this impossible --
            # with self.assertRaises(RuntimeError) as ex1:
            #     session.query(object_type=Dto).vector_search_i8("EmbeddingField", [2.5, 3.3], 0.65)
            # self.assertIn("Cannot quantize already quantized embeddings", str(ex1.exception))
            #
            # with self.assertRaises(RuntimeError) as ex2:
            #     session.query(object_type=Dto).vector_search_i8("EmbeddingField", [2.5, 3.3], 0.65,
            #                                                           target_quantization="single")
            # self.assertIn("Cannot quantize already quantized embeddings", str(ex2.exception))

            q1 = session.query(object_type=Dto).vector_search_i8("EmbeddingField", [2.5, 3.3], 0.65)
            self.assertEqual(
                "from 'Dtoes' where vector.search(embedding.i8(EmbeddingField), $p0, 0.65, null)", q1._to_string()
            )

            q2 = session.query(object_type=Dto).vector_search_f32_i8("EmbeddingField", [2.5, 3.3], 0.65)
            self.assertEqual(
                "from 'Dtoes' where vector.search(embedding.f32_i8(EmbeddingField), $p0, 0.65, null)", q2._to_string()
            )

            q3 = session.query(object_type=Dto).vector_search_f32_i8("EmbeddingField", "abcd==", 0.75)
            self.assertEqual(
                "from 'Dtoes' where vector.search(embedding.f32_i8(EmbeddingField), $p0, 0.75, null)", q3._to_string()
            )

            q4 = session.query(object_type=Dto).vector_search_text("TextField", "abc")
            self.assertEqual("from 'Dtoes' where vector.search(embedding.text(TextField), $p0)", q4._to_string())

            q5 = session.query(object_type=Dto).vector_search_i1("Base64Field", "ddddd==", 0.85)
            self.assertEqual(
                "from 'Dtoes' where vector.search(embedding.i1(Base64Field), $p0, 0.85, null)", q5._to_string()
            )

            q6 = session.query(object_type=Dto).vector_search_i8("Base64Field", [0.2, 0.3])
            self.assertEqual("from 'Dtoes' where vector.search(embedding.i8(Base64Field), $p0)", q6._to_string())

            q7 = session.query(object_type=Dto).vector_search("EmbeddingBase64", "abcd==")
            self.assertEqual("from 'Dtoes' where vector.search(EmbeddingBase64, $p0)", q7._to_string())

            q8 = session.query(object_type=Dto).vector_search(
                "EmbeddingBase64", "abcd==", is_exact=True, number_of_candidates=25
            )
            self.assertEqual("from 'Dtoes' where exact(vector.search(EmbeddingBase64, $p0, null, 25))", q8._to_string())
