"""Tests for GenAI task operations."""

import os
import unittest

from ravendb.documents.operations.ai.ai_connection_string import AiConnectionString, AiModelType
from ravendb.documents.operations.ai.add_gen_ai_operation import AddGenAiOperation
from ravendb.documents.operations.ai.gen_ai_configuration import GenAiConfiguration
from ravendb.documents.operations.ai.gen_ai_transformation import GenAiTransformation
from ravendb.documents.operations.ai.update_gen_ai_operation import UpdateGenAiOperation
from ravendb.documents.operations.ai.open_ai_settings import OpenAiSettings
from ravendb.documents.operations.connection_string.put_connection_string_operation import PutConnectionStringOperation
from ravendb.documents.operations.connection_string.remove_connection_string_operation import (
    RemoveConnectionStringOperation,
)
from ravendb.documents.operations.ongoing_tasks import (
    DeleteOngoingTaskOperation,
    GetOngoingTaskInfoOperation,
    OngoingTaskType,
    OngoingTaskGenAi,
)
from ravendb.documents.starting_point_change_vector import StartingPointChangeVector
from ravendb.tests.test_base import TestBase


class TestGenAiConfigurationValidation(unittest.TestCase):
    """Tests for GenAiConfiguration.validate() method. No server required."""

    def test_validate_valid_configuration_with_sample_object(self):
        """Validates that a complete configuration with sample_object passes validation."""
        config = GenAiConfiguration(
            name="TestGenAi",
            identifier="test-gen-ai-1",
            collection="Documents",
            connection_string_name="my-connection",
            prompt="Test prompt",
            gen_ai_transformation=GenAiTransformation(script="ai.genContext(ctx);"),
            update_script="this.Summary = $result;",
            sample_object='{"summary": "test"}',
        )
        errors = config.validate()
        self.assertEqual(0, len(errors), f"Expected no errors, got: {errors}")

    def test_validate_valid_configuration_with_json_schema(self):
        """Validates that a complete configuration with json_schema passes validation."""
        config = GenAiConfiguration(
            name="TestGenAi",
            identifier="test-gen-ai-1",
            collection="Documents",
            connection_string_name="my-connection",
            prompt="Test prompt",
            gen_ai_transformation=GenAiTransformation(script="ai.genContext(ctx);"),
            update_script="this.Summary = $result;",
            json_schema='{"type": "object", "properties": {"summary": {"type": "string"}}}',
        )
        errors = config.validate()
        self.assertEqual(0, len(errors), f"Expected no errors, got: {errors}")

    def test_validate_missing_sample_object_and_json_schema(self):
        """Validates that missing both sample_object and json_schema returns error."""
        config = GenAiConfiguration(
            name="TestGenAi",
            identifier="test-gen-ai-1",
            collection="Documents",
            connection_string_name="my-connection",
            prompt="Test prompt",
            gen_ai_transformation=GenAiTransformation(script="ai.genContext(ctx);"),
            update_script="this.Summary = $result;",
        )
        errors = config.validate()
        self.assertIn("You must provide either a JSON schema or a sample object", errors)

    def test_validate_missing_update_script(self):
        """Validates that missing update_script returns error."""
        config = GenAiConfiguration(
            name="TestGenAi",
            identifier="test-gen-ai-1",
            collection="Documents",
            connection_string_name="my-connection",
            prompt="Test prompt",
            gen_ai_transformation=GenAiTransformation(script="ai.genContext(ctx);"),
            sample_object='{"summary": "test"}',
        )
        errors = config.validate()
        self.assertIn("You must provide an update function", errors)

    def test_validate_missing_prompt(self):
        """Validates that missing prompt returns error."""
        config = GenAiConfiguration(
            name="TestGenAi",
            identifier="test-gen-ai-1",
            collection="Documents",
            connection_string_name="my-connection",
            gen_ai_transformation=GenAiTransformation(script="ai.genContext(ctx);"),
            update_script="this.Summary = $result;",
            sample_object='{"summary": "test"}',
        )
        errors = config.validate()
        self.assertIn("Prompt must be provided", errors)

    def test_validate_missing_collection(self):
        """Validates that missing collection returns error."""
        config = GenAiConfiguration(
            name="TestGenAi",
            identifier="test-gen-ai-1",
            connection_string_name="my-connection",
            prompt="Test prompt",
            gen_ai_transformation=GenAiTransformation(script="ai.genContext(ctx);"),
            update_script="this.Summary = $result;",
            sample_object='{"summary": "test"}',
        )
        errors = config.validate()
        self.assertIn("Collection must be provided", errors)

    def test_validate_missing_name(self):
        """Validates that missing name returns error."""
        config = GenAiConfiguration(
            identifier="test-gen-ai-1",
            collection="Documents",
            connection_string_name="my-connection",
            prompt="Test prompt",
            gen_ai_transformation=GenAiTransformation(script="ai.genContext(ctx);"),
            update_script="this.Summary = $result;",
            sample_object='{"summary": "test"}',
        )
        errors = config.validate()
        self.assertIn("Name of GenAi configuration cannot be empty", errors)

    def test_validate_missing_gen_ai_transformation(self):
        """Validates that missing gen_ai_transformation returns error."""
        config = GenAiConfiguration(
            name="TestGenAi",
            identifier="test-gen-ai-1",
            collection="Documents",
            connection_string_name="my-connection",
            prompt="Test prompt",
            update_script="this.Summary = $result;",
            sample_object='{"summary": "test"}',
        )
        errors = config.validate()
        self.assertIn("GenAiTransformation must be specified", errors)

    def test_validate_invalid_identifier_uppercase(self):
        """Validates that uppercase letters in identifier returns error."""
        config = GenAiConfiguration(
            name="TestGenAi",
            identifier="TestGenAi",  # Invalid: uppercase
            collection="Documents",
            connection_string_name="my-connection",
            prompt="Test prompt",
            gen_ai_transformation=GenAiTransformation(script="ai.genContext(ctx);"),
            update_script="this.Summary = $result;",
            sample_object='{"summary": "test"}',
        )
        errors = config.validate()
        self.assertTrue(any("lowercase" in e for e in errors), f"Expected lowercase error, got: {errors}")

    def test_validate_invalid_identifier_special_chars(self):
        """Validates that special characters (except hyphen) in identifier returns error."""
        config = GenAiConfiguration(
            name="TestGenAi",
            identifier="test/gen_ai",  # Invalid: slash and underscore
            collection="Documents",
            connection_string_name="my-connection",
            prompt="Test prompt",
            gen_ai_transformation=GenAiTransformation(script="ai.genContext(ctx);"),
            update_script="this.Summary = $result;",
            sample_object='{"summary": "test"}',
        )
        errors = config.validate()
        self.assertTrue(any("invalid characters" in e for e in errors), f"Expected identifier error, got: {errors}")


class TestGenAiConfigurationSerialization(unittest.TestCase):
    """Tests for GenAiConfiguration serialization. No server required."""

    def test_gen_ai_configuration_to_json(self):
        """Tests that to_json() produces correct JSON structure."""
        config = GenAiConfiguration(
            name="TestGenAi",
            identifier="test-gen-ai-1",
            collection="Documents",
            connection_string_name="my-connection",
            prompt="Test prompt",
            gen_ai_transformation=GenAiTransformation(script="ai.genContext(ctx);"),
            update_script="this.Summary = $result;",
            sample_object='{"summary": "test"}',
            max_concurrency=4,
            enable_tracing=True,
        )
        json_data = config.to_json()

        self.assertEqual("TestGenAi", json_data["Name"])
        self.assertEqual("test-gen-ai-1", json_data["Identifier"])
        self.assertEqual("Documents", json_data["Collection"])
        self.assertEqual("my-connection", json_data["ConnectionStringName"])
        self.assertEqual("Test prompt", json_data["Prompt"])
        self.assertEqual("this.Summary = $result;", json_data["UpdateScript"])
        self.assertEqual('{"summary": "test"}', json_data["SampleObject"])
        self.assertEqual(4, json_data["MaxConcurrency"])
        self.assertTrue(json_data["EnableTracing"])
        self.assertEqual("GenAi", json_data["EtlType"])

    def test_gen_ai_configuration_from_json(self):
        """Tests that from_json() correctly deserializes configuration."""
        json_data = {
            "Name": "TestGenAi",
            "Identifier": "test-gen-ai-1",
            "Collection": "Documents",
            "ConnectionStringName": "my-connection",
            "Prompt": "Test prompt",
            "UpdateScript": "this.Summary = $result;",
            "SampleObject": '{"summary": "test"}',
            "MaxConcurrency": 4,
            "EnableTracing": True,
            "GenAiTransformation": {"Script": "ai.genContext(ctx);"},
        }
        config = GenAiConfiguration.from_json(json_data)

        self.assertEqual("TestGenAi", config.name)
        self.assertEqual("test-gen-ai-1", config.identifier)
        self.assertEqual("Documents", config.collection)
        self.assertEqual("my-connection", config.connection_string_name)
        self.assertEqual("Test prompt", config.prompt)
        self.assertEqual("this.Summary = $result;", config.update_script)
        self.assertEqual('{"summary": "test"}', config.sample_object)
        self.assertEqual(4, config.max_concurrency)
        self.assertTrue(config.enable_tracing)

    def test_gen_ai_configuration_round_trip(self):
        """Tests serialization -> deserialization produces equivalent object."""
        original = GenAiConfiguration(
            name="TestGenAi",
            identifier="test-gen-ai-1",
            collection="Documents",
            connection_string_name="my-connection",
            prompt="Test prompt",
            gen_ai_transformation=GenAiTransformation(script="ai.genContext(ctx);"),
            update_script="this.Summary = $result;",
            json_schema='{"type": "object"}',
            max_concurrency=8,
            enable_tracing=False,
        )
        json_data = original.to_json()
        restored = GenAiConfiguration.from_json(json_data)

        self.assertEqual(original.name, restored.name)
        self.assertEqual(original.identifier, restored.identifier)
        self.assertEqual(original.collection, restored.collection)
        self.assertEqual(original.prompt, restored.prompt)
        self.assertEqual(original.update_script, restored.update_script)
        self.assertEqual(original.json_schema, restored.json_schema)
        self.assertEqual(original.max_concurrency, restored.max_concurrency)
        self.assertEqual(original.enable_tracing, restored.enable_tracing)

    def test_version_not_in_json_when_none(self):
        """Tests that Version key is omitted from JSON when version is None."""
        config = GenAiConfiguration(
            name="TestGenAi",
            identifier="test-gen-ai-1",
            collection="Documents",
            connection_string_name="my-connection",
            prompt="Test prompt",
            gen_ai_transformation=GenAiTransformation(script="ai.genContext(ctx);"),
            update_script="this.Summary = $result;",
            sample_object='{"summary": "test"}',
        )
        json_data = config.to_json()
        self.assertNotIn("Version", json_data)

    def test_version_in_json_when_set(self):
        """Tests that Version key is included in JSON when version has a value."""
        config = GenAiConfiguration(
            name="TestGenAi",
            identifier="test-gen-ai-1",
            collection="Documents",
            connection_string_name="my-connection",
            prompt="Test prompt",
            gen_ai_transformation=GenAiTransformation(script="ai.genContext(ctx);"),
            update_script="this.Summary = $result;",
            sample_object='{"summary": "test"}',
            version=1,
        )
        json_data = config.to_json()
        self.assertEqual(1, json_data["Version"])

    def test_version_round_trip(self):
        """Tests that version survives serialization -> deserialization."""
        config = GenAiConfiguration(
            name="TestGenAi",
            identifier="test-gen-ai-1",
            collection="Documents",
            connection_string_name="my-connection",
            prompt="Test prompt",
            gen_ai_transformation=GenAiTransformation(script="ai.genContext(ctx);"),
            update_script="this.Summary = $result;",
            sample_object='{"summary": "test"}',
            version=1,
        )
        json_data = config.to_json()
        restored = GenAiConfiguration.from_json(json_data)
        self.assertEqual(1, restored.version)

    def test_version_none_round_trip(self):
        """Tests that None version survives serialization -> deserialization."""
        config = GenAiConfiguration(
            name="TestGenAi",
            identifier="test-gen-ai-1",
            collection="Documents",
            connection_string_name="my-connection",
            prompt="Test prompt",
            gen_ai_transformation=GenAiTransformation(script="ai.genContext(ctx);"),
            update_script="this.Summary = $result;",
            sample_object='{"summary": "test"}',
        )
        json_data = config.to_json()
        restored = GenAiConfiguration.from_json(json_data)
        self.assertIsNone(restored.version)


class TestGenAiTransformation(unittest.TestCase):
    """Tests for GenAiTransformation. No server required."""

    def test_gen_ai_transformation_validate_script_valid(self):
        """Tests that valid script passes validation."""
        transformation = GenAiTransformation(script="var ctx = {}; ai.genContext(ctx);")
        is_valid, error = transformation.validate_script()
        self.assertTrue(is_valid)
        self.assertEqual("", error)

    def test_gen_ai_transformation_validate_script_missing_ai_gen_context(self):
        """Tests that script without ai.genContext fails validation."""
        transformation = GenAiTransformation(script="var ctx = {};")
        is_valid, error = transformation.validate_script()
        self.assertFalse(is_valid)
        self.assertIn("ai.genContext", error)

    def test_gen_ai_transformation_to_json(self):
        """Tests transformation serialization."""
        transformation = GenAiTransformation(script="ai.genContext(ctx);")
        json_data = transformation.to_json()
        self.assertEqual("ai.genContext(ctx);", json_data["Script"])


class TestStartingPointChangeVector(unittest.TestCase):
    """Tests for StartingPointChangeVector enum. No server required."""

    def test_starting_point_change_vector_values(self):
        """Tests enum values are correct strings."""
        self.assertEqual("DoNotChange", StartingPointChangeVector.DO_NOT_CHANGE.value)
        self.assertEqual("LastDocument", StartingPointChangeVector.LAST_DOCUMENT.value)
        self.assertEqual("BeginningOfTime", StartingPointChangeVector.BEGINNING_OF_TIME.value)

    def test_starting_point_change_vector_from_value(self):
        """Tests from_value() method."""
        result = StartingPointChangeVector.from_value("LastDocument")
        self.assertEqual(StartingPointChangeVector.LAST_DOCUMENT, result)

        result = StartingPointChangeVector.from_value("BeginningOfTime")
        self.assertEqual(StartingPointChangeVector.BEGINNING_OF_TIME, result)

        result = StartingPointChangeVector.from_value("DoNotChange")
        self.assertEqual(StartingPointChangeVector.DO_NOT_CHANGE, result)


class TestAiTaskIdentifierHelper(unittest.TestCase):
    """Tests for AiTaskIdentifierHelper. No server required."""

    def test_validate_identifier_valid(self):
        """Tests that valid identifiers pass validation."""
        from ravendb.documents.operations.ai.ai_task_identifier_helper import AiTaskIdentifierHelper

        is_valid, errors = AiTaskIdentifierHelper.validate_identifier("test-gen-ai-1")
        self.assertTrue(is_valid)
        self.assertEqual(0, len(errors))

        is_valid, errors = AiTaskIdentifierHelper.validate_identifier("my-task-123")
        self.assertTrue(is_valid)
        self.assertEqual(0, len(errors))

    def test_validate_identifier_empty(self):
        """Tests that empty identifier fails validation."""
        from ravendb.documents.operations.ai.ai_task_identifier_helper import AiTaskIdentifierHelper

        is_valid, errors = AiTaskIdentifierHelper.validate_identifier("")
        self.assertFalse(is_valid)
        self.assertTrue(any("empty" in e for e in errors))

        is_valid, errors = AiTaskIdentifierHelper.validate_identifier("   ")
        self.assertFalse(is_valid)
        self.assertTrue(any("empty" in e or "whitespace" in e for e in errors))

    def test_validate_identifier_uppercase(self):
        """Tests that uppercase letters fail validation."""
        from ravendb.documents.operations.ai.ai_task_identifier_helper import AiTaskIdentifierHelper

        is_valid, errors = AiTaskIdentifierHelper.validate_identifier("TestGenAi")
        self.assertFalse(is_valid)
        self.assertTrue(any("uppercase" in e for e in errors))

    def test_validate_identifier_invalid_chars(self):
        """Tests that invalid characters fail validation."""
        from ravendb.documents.operations.ai.ai_task_identifier_helper import AiTaskIdentifierHelper

        is_valid, errors = AiTaskIdentifierHelper.validate_identifier("test/gen_ai")
        self.assertFalse(is_valid)
        self.assertTrue(any("invalid characters" in e for e in errors))

    def test_validate_identifier_consecutive_hyphens(self):
        """Tests that consecutive hyphens fail validation."""
        from ravendb.documents.operations.ai.ai_task_identifier_helper import AiTaskIdentifierHelper

        is_valid, errors = AiTaskIdentifierHelper.validate_identifier("test--gen-ai")
        self.assertFalse(is_valid)
        self.assertTrue(any("consecutive hyphens" in e for e in errors))

    def test_validate_identifier_ends_with_hyphen(self):
        """Tests that identifier ending with hyphen fails validation."""
        from ravendb.documents.operations.ai.ai_task_identifier_helper import AiTaskIdentifierHelper

        is_valid, errors = AiTaskIdentifierHelper.validate_identifier("test-gen-ai-")
        self.assertFalse(is_valid)
        self.assertTrue(any("ends with a hyphen" in e for e in errors))

    def test_generate_identifier_basic(self):
        """Tests basic identifier generation."""
        from ravendb.documents.operations.ai.ai_task_identifier_helper import AiTaskIdentifierHelper

        result = AiTaskIdentifierHelper.generate_identifier("TestGenAi")
        self.assertEqual("testgenai", result)

    def test_generate_identifier_with_spaces(self):
        """Tests identifier generation with spaces."""
        from ravendb.documents.operations.ai.ai_task_identifier_helper import AiTaskIdentifierHelper

        result = AiTaskIdentifierHelper.generate_identifier("Test Gen Ai")
        self.assertEqual("test-gen-ai", result)

    def test_generate_identifier_with_special_chars(self):
        """Tests identifier generation with special characters."""
        from ravendb.documents.operations.ai.ai_task_identifier_helper import AiTaskIdentifierHelper

        result = AiTaskIdentifierHelper.generate_identifier("Test/Gen_Ai")
        self.assertEqual("test-gen-ai", result)

    def test_generate_identifier_empty(self):
        """Tests identifier generation with empty input."""
        from ravendb.documents.operations.ai.ai_task_identifier_helper import AiTaskIdentifierHelper

        result = AiTaskIdentifierHelper.generate_identifier("")
        self.assertIsNone(result)

        result = AiTaskIdentifierHelper.generate_identifier("   ")
        self.assertIsNone(result)


@unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
class TestGenAiCrudOperations(TestBase):
    """Tests for GenAI CRUD operations (require server connection)."""

    CONNECTION_STRING_NAME = "test-ai-connection"

    def setUp(self):
        super().setUp()
        # Create AI connection string for tests
        ai_connection_string = AiConnectionString(
            name=self.CONNECTION_STRING_NAME,
            identifier="test-ai-identifier",
            openai_settings=OpenAiSettings(
                api_key="test-api-key",
                endpoint="https://api.openai.com",
                model="gpt-4",
            ),
            model_type=AiModelType.CHAT,
        )
        self.store.maintenance.send(PutConnectionStringOperation(ai_connection_string))
        self._ai_connection_string = ai_connection_string
        self._created_task_ids = []

    def tearDown(self):
        # Clean up created tasks
        for task_id in self._created_task_ids:
            try:
                self.store.maintenance.send(DeleteOngoingTaskOperation(task_id, OngoingTaskType.GEN_AI))
            except Exception:
                pass

        # Clean up connection string
        try:
            self.store.maintenance.send(RemoveConnectionStringOperation(self._ai_connection_string))
        except Exception:
            pass

        super().tearDown()

    def _create_valid_config(self, name: str, identifier: str) -> GenAiConfiguration:
        """Helper to create a valid GenAiConfiguration."""
        return GenAiConfiguration(
            name=name,
            identifier=identifier,
            collection="Documents",
            connection_string_name=self.CONNECTION_STRING_NAME,
            prompt="Summarize the document: {{content}}",
            gen_ai_transformation=GenAiTransformation(script="var ctx = {}; ai.genContext(ctx);"),
            update_script="this.Summary = $result;",
            sample_object='{"summary": "A brief summary"}',
            max_concurrency=2,
            enable_tracing=True,
        )

    def test_add_gen_ai_operation_with_default_starting_point(self):
        """Tests adding GenAI task with default LAST_DOCUMENT starting point."""
        config = self._create_valid_config("TestAddDefault", "test-add-default")

        result = self.store.maintenance.send(AddGenAiOperation(config))

        self.assertIsNotNone(result)
        self.assertGreater(result.task_id, 0)
        self._created_task_ids.append(result.task_id)

    def test_add_gen_ai_operation_with_beginning_of_time(self):
        """Tests adding GenAI task with BEGINNING_OF_TIME starting point."""
        config = self._create_valid_config("TestAddBeginning", "test-add-beginning")

        result = self.store.maintenance.send(AddGenAiOperation(config, StartingPointChangeVector.BEGINNING_OF_TIME))

        self.assertIsNotNone(result)
        self.assertGreater(result.task_id, 0)
        self._created_task_ids.append(result.task_id)

    def test_add_gen_ai_operation_returns_task_id(self):
        """Tests that add operation returns valid task ID."""
        config = self._create_valid_config("TestReturnsId", "test-returns-id")

        result = self.store.maintenance.send(AddGenAiOperation(config))

        self.assertIsNotNone(result.task_id)
        self.assertIsInstance(result.task_id, int)
        self.assertGreater(result.task_id, 0)
        self._created_task_ids.append(result.task_id)

    def test_add_gen_ai_operation_returns_identifier(self):
        """Tests that add operation returns the identifier."""
        config = self._create_valid_config("TestReturnsIdentifier", "test-returns-identifier")

        result = self.store.maintenance.send(AddGenAiOperation(config))

        self.assertEqual("test-returns-identifier", result.identifier)
        self._created_task_ids.append(result.task_id)

    def test_get_ongoing_task_info_gen_ai(self):
        """Tests retrieving GenAI task info by task ID."""
        config = self._create_valid_config("TestGetInfo", "test-get-info")
        add_result = self.store.maintenance.send(AddGenAiOperation(config))
        self._created_task_ids.append(add_result.task_id)

        task_info = self.store.maintenance.send(GetOngoingTaskInfoOperation(add_result.task_id, OngoingTaskType.GEN_AI))

        self.assertIsNotNone(task_info)
        self.assertIsInstance(task_info, OngoingTaskGenAi)
        self.assertEqual(add_result.task_id, task_info.task_id)

    def test_get_ongoing_task_info_gen_ai_by_name(self):
        """Tests retrieving GenAI task info by task name."""
        config = self._create_valid_config("TestGetByName", "test-get-by-name")
        add_result = self.store.maintenance.send(AddGenAiOperation(config))
        self._created_task_ids.append(add_result.task_id)

        task_info = self.store.maintenance.send(GetOngoingTaskInfoOperation("TestGetByName", OngoingTaskType.GEN_AI))

        self.assertIsNotNone(task_info)
        self.assertEqual("TestGetByName", task_info.task_name)

    def test_get_ongoing_task_info_returns_correct_type(self):
        """Tests that returned task has correct OngoingTaskType.GEN_AI."""
        config = self._create_valid_config("TestCorrectType", "test-correct-type")
        add_result = self.store.maintenance.send(AddGenAiOperation(config))
        self._created_task_ids.append(add_result.task_id)

        task_info = self.store.maintenance.send(GetOngoingTaskInfoOperation(add_result.task_id, OngoingTaskType.GEN_AI))

        self.assertEqual(OngoingTaskType.GEN_AI, task_info.task_type)

    def test_update_gen_ai_operation_basic(self):
        """Tests basic update of GenAI task."""
        config = self._create_valid_config("TestUpdateBasic", "test-update-basic")
        add_result = self.store.maintenance.send(AddGenAiOperation(config))
        self._created_task_ids.append(add_result.task_id)

        # Update configuration
        config.prompt = "Updated prompt: Analyze the document"
        config.task_id = add_result.task_id

        update_result = self.store.maintenance.send(UpdateGenAiOperation(add_result.task_id, config))
        self._created_task_ids.append(update_result.task_id)
        self.assertIsNotNone(update_result)
        self.assertNotEqual(add_result.task_id, update_result.task_id)

    def test_update_gen_ai_operation_with_reset(self):
        """Tests update with reset=True to reprocess documents."""
        config = self._create_valid_config("TestUpdateReset", "test-update-reset")
        add_result = self.store.maintenance.send(AddGenAiOperation(config))
        self._created_task_ids.append(add_result.task_id)

        # Update with reset
        config.prompt = "Reset prompt: Re-analyze all documents"
        config.task_id = add_result.task_id

        update_result = self.store.maintenance.send(UpdateGenAiOperation(add_result.task_id, config, reset=True))

        self._created_task_ids.append(update_result.task_id)
        self.assertIsNotNone(update_result)
        self.assertNotEqual(add_result.task_id, update_result.task_id)

    def test_update_gen_ai_operation_with_starting_point(self):
        """Tests update with custom starting point."""
        config = self._create_valid_config("TestUpdateStartPoint", "test-update-start-point")
        add_result = self.store.maintenance.send(AddGenAiOperation(config))
        self._created_task_ids.append(add_result.task_id)

        # Update with BEGINNING_OF_TIME starting point
        config.prompt = "Updated prompt with new starting point"
        config.task_id = add_result.task_id

        update_result = self.store.maintenance.send(
            UpdateGenAiOperation(
                add_result.task_id,
                config,
                starting_point=StartingPointChangeVector.BEGINNING_OF_TIME,
            )
        )

        self._created_task_ids.append(update_result.task_id)
        self.assertIsNotNone(update_result)

    def test_delete_ongoing_task_gen_ai(self):
        """Tests deleting GenAI task."""
        config = self._create_valid_config("TestDelete", "test-delete")
        add_result = self.store.maintenance.send(AddGenAiOperation(config))

        # Delete the task
        self.store.maintenance.send(DeleteOngoingTaskOperation(add_result.task_id, OngoingTaskType.GEN_AI))

        # Verify task is deleted by trying to get it
        task_info = self.store.maintenance.send(GetOngoingTaskInfoOperation(add_result.task_id, OngoingTaskType.GEN_AI))
        self.assertIsNone(task_info)

    def test_gen_ai_full_lifecycle(self):
        """Tests complete lifecycle: add -> get -> update -> get -> delete."""
        # 1. Add task
        config = self._create_valid_config("TestLifecycle", "test-lifecycle")
        add_result = self.store.maintenance.send(AddGenAiOperation(config))
        self.assertGreater(add_result.task_id, 0)
        self.assertEqual("test-lifecycle", add_result.identifier)

        # 2. Get task
        task_info = self.store.maintenance.send(GetOngoingTaskInfoOperation(add_result.task_id, OngoingTaskType.GEN_AI))
        self.assertIsNotNone(task_info)
        self.assertEqual("TestLifecycle", task_info.task_name)

        # 3. Update task
        config.prompt = "Updated lifecycle prompt"
        config.max_concurrency = 8
        config.task_id = add_result.task_id

        update_result = self.store.maintenance.send(UpdateGenAiOperation(add_result.task_id, config))
        self.assertNotEqual(add_result.task_id, update_result.task_id)

        self._created_task_ids.append(update_result.task_id)
        # 4. Get updated task
        updated_task_info = self.store.maintenance.send(
            GetOngoingTaskInfoOperation(update_result.task_id, OngoingTaskType.GEN_AI)
        )
        self.assertIsNotNone(updated_task_info)

        # 5. Delete task
        self.store.maintenance.send(DeleteOngoingTaskOperation(updated_task_info.task_id, OngoingTaskType.GEN_AI))

        # 6. Verify deleted
        deleted_task_info = self.store.maintenance.send(
            GetOngoingTaskInfoOperation(updated_task_info.task_id, OngoingTaskType.GEN_AI)
        )
        self.assertIsNone(deleted_task_info)
