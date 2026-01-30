import unittest
from datetime import timedelta
from typing import List

from ravendb import AbstractIndexCreationTask, GetIndexesOperation
from ravendb.documents.indexes.definitions import SearchEngineType
from ravendb.documents.indexes.vector.embedding import VectorEmbeddingType
from ravendb.documents.indexes.vector.options import VectorOptions
from ravendb.documents.operations.server_misc import ToggleDatabasesStateOperation
from ravendb.infrastructure.orders import Product
from ravendb.tests.test_base import TestBase


class Dto:
    def __init__(
        self,
        embedding_base_64: str = None,
        embedding_singles: List[float] = None,
        embedding_sbytes: List[int] = None,
        embedding_binary: List[int] = None,
    ):
        self.embedding_base_64 = embedding_base_64
        self.embedding_singles = embedding_singles
        self.embedding_sbytes = embedding_sbytes
        self.embedding_binary = embedding_binary


class DummyIndex(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = """
        from dto in docs.Dtoes
        select new 
        { 
            Singles = CreateVector(dto.embedding_singles), 
            Integers = CreateVector(dto.embedding_sbytes), 
            Binary = CreateVector(dto.embedding_binary) 
        }
        """
        self._vector("Integers", VectorOptions(VectorEmbeddingType.INT8))
        self._vector("Binary", VectorOptions(VectorEmbeddingType.BINARY))
        self.search_engine_type = SearchEngineType.CORAX


class IndexWithSetDimensions(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = """
        from dto in docs.Dtoes
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
            from dto in docs.Dtoes
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

            q5 = session.query(object_type=Dto).vector_search_text("TextField", "aaaa", target_quantization=VectorEmbeddingType.INT8)
            self.assertEqual("from 'Dtoes' where vector.search(embedding.text_i8(TextField), $p0)", q5._to_string())

            q6 = session.query(object_type=Dto).vector_search_i8("EmbeddingField", [2, 3], 0.65)
            self.assertEqual(
                "from 'Dtoes' where vector.search(embedding.i8(EmbeddingField), $p0, 0.65, null)", q6._to_string()
            )

            q7 = session.query(object_type=Dto).vector_search_text("TextField", "aaaa", target_quantization=VectorEmbeddingType.INT8)
            self.assertEqual("from 'Dtoes' where vector.search(embedding.text_i8(TextField), $p0)", q7._to_string())

            # q8 = session.query(object_type=Dto).vector_search_with_field()

    def test_rql_generation_2(self):
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

            q2 = session.query(object_type=Dto).vector_search("EmbeddingField", [2.5, 3.3], 0.65, target_quantization=VectorEmbeddingType.INT8
            )
            self.assertEqual(
                "from 'Dtoes' where vector.search(embedding.f32_i8(EmbeddingField), $p0, 0.65, null)", q2._to_string()
            )

            q3 = session.query(object_type=Dto).vector_search("EmbeddingField", "abcd==", 0.75, target_quantization=VectorEmbeddingType.INT8)
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

    def test_rql_generation_3(self):
        with self.store.open_session() as session:
            # forDocument - text/field
            q1 = session.query(object_type=Dto).vector_search_with_field_for_document("VectorField", "docs/1-A")
            self.assertEqual("from 'Dtoes' where vector.search(VectorField, embedding.forDoc($p0))", q1._to_string())

            q2 = session.query(object_type=Dto).vector_search_text_for_document("VectorField", "docs/1-A", target_quantization=VectorEmbeddingType.INT8)
            self.assertEqual(
                "from 'Dtoes' where vector.search(embedding.text_i8(VectorField), embedding.forDoc($p0))", q2._to_string()
            )

            # withField
            q3 = session.query(object_type=Dto).vector_search_with_field("VectorField", [0.1, 0.2, 0.3])
            self.assertEqual("from 'Dtoes' where vector.search(VectorField, $p0)", q3._to_string())

            q4 = session.query(object_type=Dto).vector_search_with_text_field("VectorField", "hello")
            self.assertEqual("from 'Dtoes' where vector.search(VectorField, $p0)", q4._to_string())

            q5 = session.query(object_type=Dto).vector_search_with_i8_field("VectorField", [1, 2, 3])
            self.assertEqual("from 'Dtoes' where vector.search(VectorField, $p0)", q5._to_string())

            q6 = session.query(object_type=Dto).vector_search_with_i1_field("VectorField", [0, 1, 0])
            self.assertEqual("from 'Dtoes' where vector.search(VectorField, $p0)", q6._to_string())

            # with base64
            q7 = session.query(object_type=Dto).vector_search_with_base64("VectorField", "abcd==")
            self.assertEqual("from 'Dtoes' where vector.search(VectorField, $p0)", q7._to_string())

            q8 = session.query(object_type=Dto).vector_search_with_base64_i8("VectorField", "abcd==")
            self.assertEqual("from 'Dtoes' where vector.search(embedding.i8(VectorField), $p0)", q8._to_string())

            q9 = session.query(object_type=Dto).vector_search_with_base64_i1("VectorField", "abcd==")
            self.assertEqual("from 'Dtoes' where vector.search(embedding.i1(VectorField), $p0)", q9._to_string())

            # ability to search in base64
            q10 = session.query(object_type=Dto).vector_search("VectorField", "abcd==")
            self.assertEqual("from 'Dtoes' where vector.search(VectorField, $p0)", q10._to_string())

            q11 = session.query(object_type=Dto).vector_search_i8("VectorField", "abcd==")
            self.assertEqual("from 'Dtoes' where vector.search(embedding.i8(VectorField), $p0)", q11._to_string())

            q12 = session.query(object_type=Dto).vector_search_i1("VectorField", "abcd==")
            self.assertEqual("from 'Dtoes' where vector.search(embedding.i1(VectorField), $p0)", q12._to_string())

            q13 = session.query(object_type=Dto).vector_search_with_field("VectorField", "abcd==")
            self.assertEqual("from 'Dtoes' where vector.search(VectorField, $p0)", q13._to_string())

            q14 = session.query(object_type=Dto).vector_search_with_i8_field("VectorField", "abcd==")
            self.assertEqual("from 'Dtoes' where vector.search(VectorField, $p0)", q14._to_string())

            q15 = session.query(object_type=Dto).vector_search_with_i1_field("VectorField", "abcd==")
            self.assertEqual("from 'Dtoes' where vector.search(VectorField, $p0)", q15._to_string())

            # embeddingTaskIdentifier
            q16 = session.query(object_type=Dto).vector_search_text("VectorField", "hello", embedding_generation_task_identifier="my-ai-task")
            self.assertEqual(
                "from 'Dtoes' where vector.search(embedding.text(VectorField, ai.task('my-ai-task')), $p0)",
                q16._to_string(),
            )

            q17 = session.query(object_type=Dto).vector_search_text_for_document("VectorField", "hello", embedding_generation_task_identifier="my-ai-task")
            self.assertEqual(
                "from 'Dtoes' where vector.search(embedding.text(VectorField, ai.task('my-ai-task')), embedding.forDoc($p0))",
                q17._to_string(),
            )

    def test_embedding_dimensions_check(self):
        with self.store.open_session() as session:
            dto1 = Dto(embedding_singles=[0.5, -1.0])
            dto2 = Dto(embedding_singles=[0.2, 0.3])

            session.store(dto1)
            session.store(dto2)

            session.save_changes()

            index = DummyIndex()

            index.execute(self.store)

            self.wait_for_indexing(self.store)

            database_disable_result = self.store.maintenance.server.send(
                ToggleDatabasesStateOperation(self.store.database, True)
            )

            self.assertTrue(database_disable_result.success)
            self.assertTrue(database_disable_result.disabled)
            self.assertEqual(database_disable_result.name, self.store.database)

            database_enable_result = self.store.maintenance.server.send(
                ToggleDatabasesStateOperation(self.store.database, False)
            )

            self.assertTrue(database_enable_result.success)
            self.assertFalse(database_enable_result.disabled)
            self.assertEqual(database_enable_result.name, self.store.database)

            dto3 = Dto(embedding_singles=[0.1, 0.2])
            session.store(dto3)
            session.save_changes()

            self.wait_for_indexing(self.store)

            dto4 = Dto(embedding_singles=[0.5, 0.7, 0.9])
            session.store(dto4)
            session.save_changes()

            index_errors = self.wait_for_indexing_errors(self.store, timeout=timedelta(seconds=5))

            self.assertEqual(1, len(index_errors))
            self.assertIn(
                "Attempted to index embedding with 3 dimensions, but field Singles already contains indexed embedding with 2 dimensions, or was explicitly configured for embeddings with 2 dimensions.",
                index_errors[0].errors[0].error,
            )

    def test_auto_index_creation_with_exact_search(self):
        with self.store.open_session() as session:
            dto1 = Dto(embedding_singles=[0.2, 0.3])
            queried_embedding = [0.2, 0.3]
            session.store(dto1)

            session.save_changes()

            _ = list(
                session.query(object_type=Dto).vector_search(
                    embedding_field="embedding_singles", vector=queried_embedding, is_exact=True
                )
            )

            index_definitions = self.store.maintenance.send(GetIndexesOperation(0, 10))

            self.assertEqual(1, len(index_definitions))
            self.assertEqual("Auto/Dtoes/ByVector.search(embedding_singles)", index_definitions[0].name)

    def test_auto_index_creation_with_exact_search_quantized_binary(self):
        with self.store.open_session() as session:
            dto1 = Dto(embedding_binary=[0, 1, 0])
            dto2 = Dto(embedding_binary=[0, 0, 1])
            queried_embedding = [1, 1, 0]
            session.store(dto1)
            session.store(dto2)
            session.save_changes()

            results = list(
                session.query(object_type=Dto)
                .vector_search_i1("embedding_binary", queried_embedding, is_exact=True)
                .order_by_score()
            )

            self.assertEqual(2, len(results))
            self.assertEqual([0, 1, 0], results[0].embedding_binary)

            index_definitions = self.store.maintenance.send(GetIndexesOperation(0, 10))

            self.assertEqual(1, len(index_definitions))
            self.assertEqual("Auto/Dtoes/ByVector.search(embedding.i1(embedding_binary))", index_definitions[0].name)

    def test_auto_index_creation_with_exact_search_quantized_int8(self):
        with self.store.open_session() as session:
            dto1 = Dto(embedding_sbytes=[64, -127, 0, 0, -128, 63])
            dto2 = Dto(embedding_sbytes=[91, 127, 51, 51, 51, 63])
            queried_embedding = [78, 0, 43, 43, 0, 63]
            session.store(dto1)
            session.store(dto2)
            session.save_changes()

            results = list(
                session.query(object_type=Dto)
                .vector_search_i8("embedding_sbytes", queried_embedding, minimum_similarity=0)
                .order_by_score()
            )

            self.assertEqual(2, len(results))
            self.assertEqual([91, 127, 51, 51, 51, 63], results[0].embedding_sbytes)

            index_definitions = self.store.maintenance.send(GetIndexesOperation(0, 10))

            self.assertEqual(1, len(index_definitions))
            self.assertEqual("Auto/Dtoes/ByVector.search(embedding.i8(embedding_sbytes))", index_definitions[0].name)

    @unittest.skip
    def test_auto_index_creation_with_exact_search_text(self):
        with self.store.open_session() as session:
            session.store(Product(name="Bicycle"))
            session.store(Product(name="Paddle"))
            session.store(Product(name="Sea"))
            session.store(Product(name="Sailors"))
            session.store(Product(name="Oblivion"))
            session.save_changes()

            results = list(session.query(object_type=Product).vector_search_text("name", "sea").order_by_score())
            self.assertEqual(3, len(results))
            self.assertEqual("Sea", results[0].name)
            self.assertEqual("Sailors", results[1].name)
            self.assertEqual("Paddle", results[2].name)

            session.store(Product(name="Scott Steiner"))
            session.save_changes()

            results = list(
                session.query(object_type=Product)
                .vector_search_text("name", "sea", minimum_similarity=0)
                .order_by_score()
            )
            self.assertEqual(6, len(results))
            self.assertEqual("Sea", results[0].name)
            self.assertEqual("Sailors", results[1].name)
            self.assertEqual("Paddle", results[2].name)
            self.assertEqual("Scott Steiner", results[3].name)
            self.assertEqual("Bicycle", results[4].name)
            self.assertEqual("Oblivion", results[5].name)

            index_definitions = self.store.maintenance.send(GetIndexesOperation(0, 10))

            self.assertEqual(1, len(index_definitions))
            self.assertEqual("Auto/Products/ByVector.search(embedding.text(name))", index_definitions[0].name)
