"""
Tests for qualifying where-token field names with a query alias, which 7.2.6 made
overridable so a vector search does not lose its settings on the way through.
"""

import unittest

from ravendb.documents.indexes.vector.options import VectorEmbeddingType
from ravendb.documents.session.tokens.query_tokens.definitions import (
    MoreLikeThisToken,
    VectorSearchToken,
    WhereOperator,
    WhereToken,
)


def _vector_token(field_name: str = "Vector") -> VectorSearchToken:
    return VectorSearchToken(
        wrapped_field_name=field_name,
        parameter_name="p0",
        source_quantization_type=VectorEmbeddingType.SINGLE,
        target_quantization_type=VectorEmbeddingType.SINGLE,
        similarity_threshold=0.8,
        number_of_candidates_for_querying=32,
        is_exact=True,
    )


def _render(token) -> str:
    writer = []
    token.write_to(writer)
    return "".join(writer)


class TestWhereTokenAliasing(unittest.TestCase):
    def test_a_plain_where_token_gains_the_alias(self):
        token = WhereToken.create(WhereOperator.EQUALS, "Name", "p0")

        self.assertEqual("x.Name", token.add_alias("x").field_name)

    def test_the_document_id_field_is_left_alone(self):
        token = WhereToken.create(WhereOperator.EQUALS, "id()", "p0")

        self.assertIs(token, token.add_alias("x"))

    def test_add_alias_does_not_mutate_the_original(self):
        # It returns a new token, which is why callers have to use the return value.
        token = WhereToken.create(WhereOperator.EQUALS, "Name", "p0")
        aliased = token.add_alias("x")

        self.assertIsNot(token, aliased)
        self.assertEqual("Name", token.field_name)


class TestVectorSearchTokenAliasing(unittest.TestCase):
    def test_aliasing_keeps_the_token_type(self):
        # The base implementation would hand back a plain WhereToken.
        self.assertIsInstance(_vector_token().add_alias("x"), VectorSearchToken)

    def test_aliasing_qualifies_the_field(self):
        self.assertEqual("x.Vector", _vector_token().add_alias("x").field_name)

    def test_aliasing_keeps_every_vector_setting(self):
        aliased = _vector_token().add_alias("x")

        self.assertEqual(0.8, aliased._similarity_threshold)
        self.assertEqual(32, aliased._number_of_candidates_for_querying)
        self.assertTrue(aliased._is_exact)

    def test_the_aliased_token_still_renders_as_a_vector_search(self):
        self.assertEqual("exact(vector.search(x.Vector, $p0, 0.8, 32))", _render(_vector_token().add_alias("x")))

    def test_the_document_id_field_is_left_alone(self):
        token = _vector_token("id()")

        self.assertIs(token, token.add_alias("x"))

    def test_a_task_backed_token_keeps_its_task(self):
        token = VectorSearchToken(
            wrapped_field_name="Vector",
            parameter_name="p0",
            source_quantization_type=VectorEmbeddingType.SINGLE,
            target_quantization_type=VectorEmbeddingType.SINGLE,
            task_name="embeddings-task",
        )

        self.assertEqual("embeddings-task", token.add_alias("x")._task_name)


class TestMoreLikeThisToken(unittest.TestCase):
    def test_it_is_not_a_where_token_here(self):
        # C# had to override AddAlias on it because MoreLikeThisToken derives from
        # WhereToken there. In this client it does not, so alias rewriting skips it.
        self.assertFalse(issubclass(MoreLikeThisToken, WhereToken))
