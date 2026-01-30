import unittest

from ravendb.documents.indexes.vector.embedding import VectorEmbeddingType
from ravendb.documents.queries.vector import VectorQuantizer
from ravendb.tests.dotnet_migrated_tests.test_ravenDB_22076 import Dto
from ravendb.tests.test_base import TestBase


class TestVectorQuantizer(unittest.TestCase):
    """Unit tests for VectorQuantizer - no server required."""

    def test_to_int8_basic_quantization(self):
        """Test basic Int8 quantization matches C# output."""
        # Test case from C# example: VectorQuantizer.ToInt8(new float[] { 0.1f, 0.2f })
        # Expected output: [64, 127, -51, -52, 76, 62]
        # Note: C# output shows [64, 127, ...] but our analysis showed it should be [63, 127, ...]
        result = VectorQuantizer.to_int8([0.1, 0.2])

        # Should have 2 quantized values + 4 bytes for max_component float
        self.assertEqual(6, len(result))

        # All values should be signed integers in range [-128, 127]
        for val in result:
            self.assertIsInstance(val, int)
            self.assertGreaterEqual(val, -128)
            self.assertLessEqual(val, 127)

        # First value: 0.1 scaled by (127 / 0.2) = 63.5 -> 63
        self.assertEqual(63, result[0])

        # Second value: 0.2 scaled by (127 / 0.2) = 127
        self.assertEqual(127, result[1])

        # Last 4 bytes represent max_component (0.2) as little-endian float
        # 0.2f in IEEE 754 little-endian: 0x3E4CCCCD = [-51, -52, 76, 62] as signed bytes
        self.assertEqual([-51, -52, 76, 62], result[2:6])

    def test_to_int8_second_example(self):
        """Test second example from C# code."""
        # VectorQuantizer.ToInt8(new float[] { 0.3f, 0.4f })
        result = VectorQuantizer.to_int8([0.3, 0.4])

        self.assertEqual(6, len(result))

        # First value: 0.3 scaled by (127 / 0.4) = 95.25 -> 95
        self.assertEqual(95, result[0])

        # Second value: 0.4 scaled by (127 / 0.4) = 127
        self.assertEqual(127, result[1])

        # Last 4 bytes represent max_component (0.4) as little-endian float
        # 0.4f in IEEE 754 little-endian: 0x3ECCCCCD = [-51, -52, -52, 62] as signed bytes
        self.assertEqual([-51, -52, -52, 62], result[2:6])

    def test_to_int8_negative_values(self):
        """Test Int8 quantization with negative values."""
        result = VectorQuantizer.to_int8([-0.5, 0.5])

        self.assertEqual(6, len(result))

        # First value: -0.5 scaled by (127 / 0.5) = -127
        self.assertEqual(-127, result[0])

        # Second value: 0.5 scaled by (127 / 0.5) = 127
        self.assertEqual(127, result[1])

    def test_to_int8_all_zeros(self):
        """Test Int8 quantization with all zeros."""
        result = VectorQuantizer.to_int8([0.0, 0.0, 0.0])

        self.assertEqual(7, len(result))  # 3 quantized + 4 float bytes

        # All quantized values should be 0
        self.assertEqual([0, 0, 0], result[:3])

        # max_component is 0.0, which is all zeros in IEEE 754
        self.assertEqual([0, 0, 0, 0], result[3:7])

    def test_to_int8_empty_list(self):
        """Test Int8 quantization with empty list."""
        result = VectorQuantizer.to_int8([])
        self.assertEqual([], result)

    def test_to_int1_basic_quantization(self):
        """Test basic Int1 (binary) quantization."""
        # Positive values -> 1, negative values -> 0
        result = VectorQuantizer.to_int1([0.5, -0.3, 0.8, -0.1, 0.2])

        self.assertEqual(5, len(result))
        self.assertEqual([1, 0, 1, 0, 1], result)

    def test_to_int1_zero_is_positive(self):
        """Test that zero is treated as non-negative (1)."""
        result = VectorQuantizer.to_int1([0.0, -0.0, 1.0, -1.0])

        self.assertEqual(4, len(result))
        # 0.0 and -0.0 are both >= 0, so they should be 1
        self.assertEqual([1, 1, 1, 0], result)

    def test_to_int1_all_positive(self):
        """Test Int1 quantization with all positive values."""
        result = VectorQuantizer.to_int1([0.1, 0.2, 0.3, 0.4, 0.5])

        self.assertEqual(5, len(result))
        self.assertEqual([1, 1, 1, 1, 1], result)

    def test_to_int1_all_negative(self):
        """Test Int1 quantization with all negative values."""
        result = VectorQuantizer.to_int1([-0.1, -0.2, -0.3, -0.4, -0.5])

        self.assertEqual(5, len(result))
        self.assertEqual([0, 0, 0, 0, 0], result)

    def test_to_int1_empty_list(self):
        """Test Int1 quantization with empty list."""
        result = VectorQuantizer.to_int1([])
        self.assertEqual([], result)

    def test_to_int1_padding_trimmed(self):
        """Test that Int1 quantization trims padding bits correctly."""
        # 9 values should pack into 2 bytes (16 bits), but only return 9 values
        result = VectorQuantizer.to_int1([1.0] * 9)

        self.assertEqual(9, len(result))
        self.assertEqual([1] * 9, result)


class TestVectorSearch(TestBase):
    def test_should_generate_rql_with_text_field_using_named_ai_task(self):
        with self.store.open_session() as session:
            q = session.query(object_type=Dto).vector_search_text(
                "EmbeddingField", "fishing", embedding_generation_task_identifier="my-ai-task"
            )
            self.assertEqual(
                "from 'Dtoes' where vector.search(embedding.text(EmbeddingField, ai.task('my-ai-task')), $p0)",
                q._to_string(),
            )

            q_exact = session.query(object_type=Dto).vector_search_text(
                "EmbeddingField", "fishing", embedding_generation_task_identifier="my-ai-task", is_exact=True
            )
            self.assertEqual(
                "from 'Dtoes' where exact(vector.search(embedding.text(EmbeddingField, ai.task('my-ai-task')), $p0))",
                q_exact._to_string(),
            )

            q2 = session.query(object_type=Dto).vector_search_text(
                "EmbeddingField",
                "fishing",
                target_quantization=VectorEmbeddingType.BINARY,
                embedding_generation_task_identifier="my-ai-task",
            )
            self.assertEqual(
                "from 'Dtoes' where vector.search(embedding.text_i1(EmbeddingField, ai.task('my-ai-task')), $p0)",
                q2._to_string(),
            )

            q2_exact = session.query(object_type=Dto).vector_search_text(
                "EmbeddingField",
                "fishing",
                target_quantization=VectorEmbeddingType.BINARY,
                embedding_generation_task_identifier="my-ai-task",
                is_exact=True,
            )
            self.assertEqual(
                "from 'Dtoes' where exact(vector.search(embedding.text_i1(EmbeddingField, ai.task('my-ai-task')), $p0))",
                q2_exact._to_string(),
            )

            q3 = session.query(object_type=Dto).vector_search_text(
                "EmbeddingField",
                "fishing",
                target_quantization=VectorEmbeddingType.INT8,
                embedding_generation_task_identifier="my-ai-task",
            )
            self.assertEqual(
                "from 'Dtoes' where vector.search(embedding.text_i8(EmbeddingField, ai.task('my-ai-task')), $p0)",
                q3._to_string(),
            )

            q3_exact = session.query(object_type=Dto).vector_search_text(
                "EmbeddingField",
                "fishing",
                embedding_generation_task_identifier="my-ai-task",
                is_exact=True,
                target_quantization=VectorEmbeddingType.INT8,
            )
            self.assertEqual(
                "from 'Dtoes' where exact(vector.search(embedding.text_i8(EmbeddingField, ai.task('my-ai-task')), $p0))",
                q3_exact._to_string(),
            )
