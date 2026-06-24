"""Tests for Embeddings Generation task management operations."""

import os
import unittest

from ravendb.documents.operations.ai import (
    AiConnectionString,
    AiModelType,
    ChunkingOptions,
    ChunkingMethod,
    EmbeddingPathConfiguration,
    EmbeddingsGenerationConfiguration,
    AddEmbeddingsGenerationOperation,
    UpdateEmbeddingsGenerationOperation,
)
from ravendb.documents.operations.ai.embedded_settings import EmbeddedSettings
from ravendb.documents.operations.ai.embeddings_transformation import EmbeddingsTransformation
from ravendb.documents.operations.connection_string.put_connection_string_operation import PutConnectionStringOperation
from ravendb.documents.operations.connection_string.remove_connection_string_operation import (
    RemoveConnectionStringOperation,
)
from ravendb.documents.operations.ongoing_tasks import (
    DeleteOngoingTaskOperation,
    GetOngoingTaskInfoOperation,
    OngoingTaskType,
    OngoingTaskState,
    OngoingTaskEmbeddingsGeneration,
)
from ravendb import GetDatabaseRecordOperation
from ravendb.tests.test_base import TestBase


class TestEmbeddingsGenerationConfigurationValidation(unittest.TestCase):
    """Tests for EmbeddingsGenerationConfiguration.validate() method. No server required."""

    DEFAULT_CHUNKING_OPTIONS = ChunkingOptions(
        chunking_method=ChunkingMethod.PLAIN_TEXT_SPLIT_LINES,
        max_tokens_per_chunk=2048,
    )

    def test_validate_valid_configuration_with_paths(self):
        """Validates that a complete configuration with paths passes validation."""
        config = EmbeddingsGenerationConfiguration(
            name="ai-task-testing",
            identifier="ai-task-testing",
            collection="Posts",
            connection_string_name="ai-service-connection",
            embeddings_path_configurations=[
                EmbeddingPathConfiguration(path="PostContent", chunking_options=self.DEFAULT_CHUNKING_OPTIONS),
                EmbeddingPathConfiguration(path="Comments", chunking_options=self.DEFAULT_CHUNKING_OPTIONS),
            ],
            chunking_options_for_querying=self.DEFAULT_CHUNKING_OPTIONS,
        )
        errors = config.validate()
        self.assertEqual(0, len(errors), f"Expected no errors, got: {errors}")

    def test_validate_missing_chunking_options_for_querying(self):
        """Validates that missing chunking_options_for_querying returns error."""
        config = EmbeddingsGenerationConfiguration(
            name="ai-task-testing",
            identifier="ai-task-testing",
            collection="Posts",
            connection_string_name="ai-service-connection",
            embeddings_path_configurations=[
                EmbeddingPathConfiguration(path="PostContent", chunking_options=self.DEFAULT_CHUNKING_OPTIONS),
            ],
            chunking_options_for_querying=None,
        )
        errors = config.validate()
        self.assertIn("ChunkingOptionsForQuerying must be provided", errors)

    def test_validate_missing_paths_and_transformation(self):
        """Validates that missing both paths and transformation returns error."""
        config = EmbeddingsGenerationConfiguration(
            name="ai-task-testing",
            identifier="ai-task-testing",
            collection="Posts",
            connection_string_name="ai-service-connection",
            chunking_options_for_querying=self.DEFAULT_CHUNKING_OPTIONS,
        )
        errors = config.validate()
        self.assertIn("Either EmbeddingsPathConfigurations or EmbeddingsTransformation must be provided", errors)

    def test_validate_missing_collection(self):
        """Validates that missing collection returns error."""
        config = EmbeddingsGenerationConfiguration(
            name="ai-task-testing",
            identifier="ai-task-testing",
            connection_string_name="ai-service-connection",
            embeddings_path_configurations=[
                EmbeddingPathConfiguration(path="PostContent", chunking_options=self.DEFAULT_CHUNKING_OPTIONS),
            ],
            chunking_options_for_querying=self.DEFAULT_CHUNKING_OPTIONS,
        )
        errors = config.validate()
        self.assertIn("Collection must be provided", errors)


class TestEmbeddingsGenerationTasksManagement(TestBase):
    """Tests for Embeddings Generation task CRUD operations (require server connection)."""

    CONNECTION_STRING_NAME = "ai-service-connection"

    DEFAULT_CHUNKING_OPTIONS = ChunkingOptions(
        chunking_method=ChunkingMethod.PLAIN_TEXT_SPLIT_LINES,
        max_tokens_per_chunk=2048,
    )

    def setUp(self):
        super().setUp()
        # Create AI connection string for tests
        ai_connection_string = AiConnectionString(
            name=self.CONNECTION_STRING_NAME,
            identifier="test-embeddings-identifier",
            embedded_settings=EmbeddedSettings(),
            model_type=AiModelType.TEXT_EMBEDDINGS,
        )
        self.store.maintenance.send(PutConnectionStringOperation(ai_connection_string))
        self._ai_connection_string = ai_connection_string
        self._created_task_ids = []

    def tearDown(self):
        # Clean up created tasks
        for task_id in self._created_task_ids:
            try:
                self.store.maintenance.send(DeleteOngoingTaskOperation(task_id, OngoingTaskType.EMBEDDINGS_GENERATION))
            except Exception:
                pass

        # Clean up connection string
        try:
            self.store.maintenance.send(RemoveConnectionStringOperation(self._ai_connection_string))
        except Exception:
            pass

        super().tearDown()

    def _create_valid_config(self, name: str = "ai-task-testing") -> EmbeddingsGenerationConfiguration:
        """Helper to create a valid EmbeddingsGenerationConfiguration."""
        return EmbeddingsGenerationConfiguration(
            name=name,
            connection_string_name=self.CONNECTION_STRING_NAME,
            embeddings_path_configurations=[
                EmbeddingPathConfiguration(path="PostContent", chunking_options=self.DEFAULT_CHUNKING_OPTIONS),
                EmbeddingPathConfiguration(path="Comments", chunking_options=self.DEFAULT_CHUNKING_OPTIONS),
            ],
            collection="Posts",
            chunking_options_for_querying=self.DEFAULT_CHUNKING_OPTIONS,
        )

    def test_can_delete_task(self):
        """Tests that an embeddings generation task can be deleted."""
        config = self._create_valid_config()

        # Add the task
        add_result = self.store.maintenance.send(AddEmbeddingsGenerationOperation(config))
        self.assertIsNotNone(add_result.raft_command_index)
        self.assertIsNotNone(add_result.task_id)

        # Delete the task
        self.store.maintenance.send(
            DeleteOngoingTaskOperation(add_result.task_id, OngoingTaskType.EMBEDDINGS_GENERATION)
        )

        # Verify task is deleted
        ongoing_task = self.store.maintenance.send(
            GetOngoingTaskInfoOperation(add_result.task_id, OngoingTaskType.EMBEDDINGS_GENERATION)
        )
        self.assertIsNone(ongoing_task)

    def test_can_update_task(self):
        """Tests that an embeddings generation task can be updated."""
        config = self._create_valid_config()

        # Add the task
        add_result = self.store.maintenance.send(AddEmbeddingsGenerationOperation(config))
        self.assertIsNotNone(add_result.raft_command_index)
        self.assertIsNotNone(add_result.task_id)
        self._created_task_ids.append(add_result.task_id)

        # Update the task to disabled
        config.disabled = True
        config.identifier = add_result.identifier

        update_result = self.store.maintenance.send(UpdateEmbeddingsGenerationOperation(add_result.task_id, config))

        # Verify task is updated
        ongoing_task = self.store.maintenance.send(
            GetOngoingTaskInfoOperation(update_result.task_id, OngoingTaskType.EMBEDDINGS_GENERATION)
        )
        self.assertIsNotNone(ongoing_task)
        self.assertEqual(OngoingTaskState.DISABLED, ongoing_task.task_state)

        # Update created task ids for cleanup
        self._created_task_ids.append(update_result.task_id)

    def test_embeddings_generation_configuration_without_chunking_options_should_provide_meaningful_error(self):
        """Tests that missing ChunkingOptions in path configuration provides meaningful error."""
        config = EmbeddingsGenerationConfiguration(
            name="ai-task-testing",
            connection_string_name=self.CONNECTION_STRING_NAME,
            embeddings_path_configurations=[
                EmbeddingPathConfiguration(path="PostContent"),  # No ChunkingOptions
            ],
            collection="Posts",
            chunking_options_for_querying=self.DEFAULT_CHUNKING_OPTIONS,
        )

        # Try to add the task and expect an error
        with self.assertRaises(Exception) as context:
            self.store.maintenance.send(AddEmbeddingsGenerationOperation(config))

        # Verify the error message contains meaningful information
        error_message = str(context.exception)
        self.assertIn("PostContent", error_message)
        self.assertIn("ChunkingOptions", error_message)

    def test_can_add_task_with_both_paths_and_transformation(self):
        """The server imposes no mutual-exclusivity between paths and transformation, so a config
        that sets BOTH must be accepted (the client must not reject it pre-send either)."""
        config = EmbeddingsGenerationConfiguration(
            name="ai-task-both",
            connection_string_name=self.CONNECTION_STRING_NAME,
            embeddings_path_configurations=[
                EmbeddingPathConfiguration(path="PostContent", chunking_options=self.DEFAULT_CHUNKING_OPTIONS),
            ],
            embeddings_transformation=EmbeddingsTransformation(
                script="embeddings.generate(this.Comments)",
                chunking_options=self.DEFAULT_CHUNKING_OPTIONS,
            ),
            collection="Posts",
            chunking_options_for_querying=self.DEFAULT_CHUNKING_OPTIONS,
        )

        add_result = self.store.maintenance.send(AddEmbeddingsGenerationOperation(config))
        self.assertIsNotNone(add_result.task_id)
        self._created_task_ids.append(add_result.task_id)

    def test_database_record_contains_embeddings_generations(self):
        config = self._create_valid_config()

        # Add the task
        add_result = self.store.maintenance.send(AddEmbeddingsGenerationOperation(config))
        self._created_task_ids.append(add_result.task_id)

        # Retrieve the database record
        record = self.store.maintenance.server.send(GetDatabaseRecordOperation(self.store.database))

        # Assert embeddings_generations is populated and deserialized correctly
        self.assertIsNotNone(record.embeddings_generations)
        self.assertEqual(1, len(record.embeddings_generations))

        eg_config = record.embeddings_generations[0]
        self.assertIsInstance(eg_config, EmbeddingsGenerationConfiguration)
        self.assertEqual(config.name, eg_config.name)
        self.assertEqual(config.collection, eg_config.collection)
        self.assertEqual(config.connection_string_name, eg_config.connection_string_name)
        self.assertEqual(2, len(eg_config.embeddings_path_configurations))
        self.assertIsNotNone(eg_config.chunking_options_for_querying)


if __name__ == "__main__":
    unittest.main()
