import unittest

from ravendb.documents.operations.ai.chunking_options import ChunkingOptions, ChunkingMethod
from ravendb.documents.operations.ai.embeddings_generation_configuration import (
    EmbeddingsGenerationConfiguration,
)


class TestChunkingOptionsContextPrefix(unittest.TestCase):
    def test_context_prefix_serialized(self):
        options = ChunkingOptions(ChunkingMethod.PLAIN_TEXT_SPLIT, 100, 0, context_prefix="Title: Foo")
        self.assertEqual("Title: Foo", options.to_json()["ContextPrefix"])

    def test_context_prefix_round_trip(self):
        options = ChunkingOptions(ChunkingMethod.PLAIN_TEXT_SPLIT, 100, 0, context_prefix="Title: Foo")
        restored = ChunkingOptions.from_json(options.to_json())
        self.assertEqual("Title: Foo", restored.context_prefix)
        self.assertEqual(options, restored)
        self.assertEqual(hash(options), hash(restored))

    def test_equality_is_sensitive_to_context_prefix(self):
        left = ChunkingOptions(ChunkingMethod.HTML_STRIP, 100, 0, context_prefix="A")
        right = ChunkingOptions(ChunkingMethod.HTML_STRIP, 100, 0, context_prefix="B")
        self.assertNotEqual(left, right)

    def test_validate_rejects_empty_or_whitespace_prefix(self):
        for bad in ("", "   "):
            errors = []
            ChunkingOptions(ChunkingMethod.HTML_STRIP, 100, 0, context_prefix=bad).validate("source", errors)
            self.assertEqual(1, len(errors), bad)
            self.assertIn("ContextPrefix", errors[0])

    def test_validate_accepts_valid_prefix(self):
        errors = []
        ChunkingOptions(ChunkingMethod.HTML_STRIP, 100, 0, context_prefix="Doc title").validate("source", errors)
        self.assertEqual([], errors)

    def test_no_chunking_marker_is_not_serialized(self):
        options = ChunkingOptions(ChunkingMethod.HTML_STRIP, 100, 0, context_prefix="ok")
        options.no_chunking = True
        self.assertNotIn("NoChunking", options.to_json())

    def test_no_chunking_marker_bypasses_budget_validation(self):
        # max_tokens_per_chunk == 0 would normally be rejected; no_chunking skips those checks.
        options = ChunkingOptions(ChunkingMethod.HTML_STRIP, 0, 0, context_prefix="ok")
        options.no_chunking = True
        errors = []
        options.validate("source", errors)
        self.assertEqual([], errors)

    def test_overlap_allowed_only_on_paragraph_methods(self):
        # Matches the server / C# / Node clients: overlap is consumed only by the two paragraph methods.
        for method in (ChunkingMethod.PLAIN_TEXT_SPLIT_PARAGRAPHS, ChunkingMethod.MARK_DOWN_SPLIT_PARAGRAPHS):
            errors = []
            ChunkingOptions(method, 100, 10).validate("source", errors)
            self.assertEqual([], errors, method)

    def test_overlap_rejected_on_non_paragraph_methods(self):
        for method in (
            ChunkingMethod.PLAIN_TEXT_SPLIT,
            ChunkingMethod.PLAIN_TEXT_SPLIT_LINES,
            ChunkingMethod.MARK_DOWN_SPLIT_LINES,
            ChunkingMethod.HTML_STRIP,
        ):
            errors = []
            ChunkingOptions(method, 100, 10).validate("source", errors)
            self.assertEqual(1, len(errors), method)
            self.assertIn("OverlapTokens", errors[0])

    def test_from_json_missing_int_keys_use_csharp_defaults(self):
        # Missing MaxTokensPerChunk/OverlapTokens must default to 512/0 (matching C#'s non-nullable
        # int initializers), not None. None would crash validate() with a TypeError.
        options = ChunkingOptions.from_json({"ChunkingMethod": "HtmlStrip"})
        self.assertEqual(512, options.max_tokens_per_chunk)
        self.assertEqual(0, options.overlap_tokens)

        errors = []
        options.validate("source", errors)  # must not raise TypeError on the defaults
        self.assertEqual([], errors)


if __name__ == "__main__":
    unittest.main()


class TestStoreChunkText(unittest.TestCase):
    """StoreChunkText, added in 7.2.6, keeps each chunk's text next to its embedding."""

    @staticmethod
    def _configuration(**kwargs) -> EmbeddingsGenerationConfiguration:
        return EmbeddingsGenerationConfiguration(
            name="embeddings",
            identifier="embeddings",
            collection="Orders",
            connection_string_name="ai",
            chunking_options_for_querying=ChunkingOptions(
                chunking_method=ChunkingMethod.PLAIN_TEXT_SPLIT, max_tokens_per_chunk=512
            ),
            **kwargs,
        )

    def test_it_is_off_unless_asked_for(self):
        # It costs storage, so opting in has to be deliberate.
        self.assertFalse(self._configuration().store_chunk_text)
        self.assertFalse(self._configuration().to_json()["StoreChunkText"])

    def test_it_reaches_the_wire_when_set(self):
        self.assertTrue(self._configuration(store_chunk_text=True).to_json()["StoreChunkText"])

    def test_it_round_trips(self):
        serialized = self._configuration(store_chunk_text=True).to_json()

        self.assertTrue(EmbeddingsGenerationConfiguration.from_json(serialized).store_chunk_text)
        self.assertEqual(serialized, EmbeddingsGenerationConfiguration.from_json(serialized).to_json())

    def test_a_configuration_from_an_older_server_reads_as_off(self):
        serialized = self._configuration().to_json()
        del serialized["StoreChunkText"]

        self.assertFalse(EmbeddingsGenerationConfiguration.from_json(serialized).store_chunk_text)
