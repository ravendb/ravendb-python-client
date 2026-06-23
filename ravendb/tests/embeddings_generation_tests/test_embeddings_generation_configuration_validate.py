import unittest

from ravendb.documents.operations.ai.chunking_options import ChunkingOptions, ChunkingMethod
from ravendb.documents.operations.ai.embedding_path_configuration import EmbeddingPathConfiguration
from ravendb.documents.operations.ai.embeddings_generation_configuration import EmbeddingsGenerationConfiguration
from ravendb.documents.operations.ai.embeddings_transformation import EmbeddingsTransformation


def _config(**overrides) -> EmbeddingsGenerationConfiguration:
    # A base config that passes every check except the paths/transformation logic under test.
    base = dict(
        name="emb",
        identifier="emb",
        collection="Docs",
        connection_string_name="cs",
        chunking_options_for_querying=ChunkingOptions(ChunkingMethod.HTML_STRIP, 100, 0),
    )
    base.update(overrides)
    return EmbeddingsGenerationConfiguration(**base)


def _validate(config) -> list:
    return config.validate(validate_name=False, validate_identifier=False)


class TestEmbeddingsGenerationConfigurationValidate(unittest.TestCase):
    def test_paths_and_transformation_together_are_allowed(self):
        # C# has no mutual-exclusivity rule; setting both must NOT be rejected client-side.
        config = _config(
            embeddings_path_configurations=[
                EmbeddingPathConfiguration("Name", ChunkingOptions(ChunkingMethod.HTML_STRIP, 100, 0))
            ],
            embeddings_transformation=EmbeddingsTransformation(script="embeddings.generate(this.Name)"),
        )
        errors = _validate(config)
        self.assertNotIn("Cannot specify both EmbeddingsPathConfigurations and EmbeddingsTransformation", errors)
        self.assertEqual([], errors)

    def test_path_missing_chunking_options_is_rejected(self):
        # Mirrors C#: each path must carry ChunkingOptions.
        config = _config(embeddings_path_configurations=[EmbeddingPathConfiguration("Name", None)])
        errors = _validate(config)
        self.assertIn("Path 'Name': ChunkingOptions must be provided.", errors)

    def test_path_invalid_chunking_options_is_rejected(self):
        # Each path's ChunkingOptions is validated with the path as the error source.
        config = _config(
            embeddings_path_configurations=[
                EmbeddingPathConfiguration("Name", ChunkingOptions(ChunkingMethod.HTML_STRIP, 0, 0))
            ]
        )
        errors = _validate(config)
        self.assertTrue(
            any("Name" in e and "MaxTokensPerChunk" in e for e in errors),
            f"expected a per-path MaxTokensPerChunk error, got: {errors}",
        )

    def test_path_with_valid_chunking_options_passes(self):
        config = _config(
            embeddings_path_configurations=[
                EmbeddingPathConfiguration("Name", ChunkingOptions(ChunkingMethod.HTML_STRIP, 100, 0))
            ]
        )
        self.assertEqual([], _validate(config))


if __name__ == "__main__":
    unittest.main()
