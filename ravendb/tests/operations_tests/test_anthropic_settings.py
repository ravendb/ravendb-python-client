import unittest

from ravendb.documents.operations.ai.ai_connection_string import AiConnectionString, AiConnectorType, AiModelType
from ravendb.documents.operations.ai.anthropic_settings import AnthropicSettings
from ravendb.documents.operations.connection_string.get_connection_string_operation import GetConnectionStringsOperation
from ravendb.documents.operations.connection_string.put_connection_string_operation import (
    PutConnectionStringOperation,
)
from ravendb.serverwide.server_operation_executor import ConnectionStringType
from ravendb.tests.test_base import TestBase

_MODEL = "claude-test-model"


class TestAnthropicSettingsSerialization(unittest.TestCase):
    def test_optional_fields_are_omitted_when_not_set(self):
        json_dict = AnthropicSettings("api-key", _MODEL, endpoint="  ", reasoning_effort="", api_version=" ").to_json()

        self.assertEqual("api-key", json_dict["ApiKey"])
        self.assertEqual(_MODEL, json_dict["Model"])
        for field in ["Endpoint", "MaxOutputTokens", "ReasoningEffort", "EnablePromptCache", "ApiVersion"]:
            with self.subTest(field=field):
                self.assertNotIn(field, json_dict)

    def test_json_round_trip(self):
        settings = AnthropicSettings(
            "api-key",
            _MODEL,
            endpoint="https://proxy.example.com/anthropic/",
            max_output_tokens=1000,
            reasoning_effort="high",
            enable_prompt_cache=False,
            api_version="2023-06-01",
        )

        json_dict = settings.to_json()
        self.assertEqual(1000, json_dict["MaxOutputTokens"])
        self.assertIs(False, json_dict["EnablePromptCache"])

        read = AnthropicSettings.from_json(json_dict)
        self.assertEqual(settings.api_key, read.api_key)
        self.assertEqual(settings.model, read.model)
        self.assertEqual(settings.endpoint, read.endpoint)
        self.assertEqual(settings.max_output_tokens, read.max_output_tokens)
        self.assertEqual(settings.reasoning_effort, read.reasoning_effort)
        self.assertIs(False, read.enable_prompt_cache)
        self.assertEqual(settings.api_version, read.api_version)


class TestAnthropicConnectionString(unittest.TestCase):
    def test_anthropic_is_the_active_provider(self):
        connection_string = AiConnectionString(
            name="anthropic",
            identifier="anthropic",
            model_type=AiModelType.CHAT,
            anthropic_settings=AnthropicSettings("api-key", _MODEL),
        )

        self.assertEqual(AiConnectorType.ANTHROPIC, connection_string.get_active_provider())
        self.assertEqual(_MODEL, connection_string.to_json()["AnthropicSettings"]["Model"])

    def test_default_endpoint_is_encrypted(self):
        for endpoint, expected in [(None, True), ("https://proxy.example.com", True), ("http://localhost:8080", False)]:
            with self.subTest(endpoint=endpoint):
                connection_string = AiConnectionString(
                    name="anthropic",
                    identifier="anthropic",
                    model_type=AiModelType.CHAT,
                    anthropic_settings=AnthropicSettings("api-key", _MODEL, endpoint=endpoint),
                )
                self.assertEqual(expected, connection_string.using_encrypted_communication_channel())

    def test_text_embeddings_model_type_is_rejected(self):
        with self.assertRaises(ValueError) as context:
            AiConnectionString(
                name="anthropic",
                identifier="anthropic",
                model_type=AiModelType.TEXT_EMBEDDINGS,
                anthropic_settings=AnthropicSettings("api-key", _MODEL),
            )
        self.assertIn("'Anthropic' does not support the 'TextEmbeddings' model type", str(context.exception))

    def test_missing_provider_error_lists_anthropic_settings(self):
        with self.assertRaises(ValueError) as context:
            AiConnectionString(name="no-provider", identifier="no-provider", model_type=AiModelType.CHAT)
        self.assertIn("anthropic_settings", str(context.exception))


class TestAnthropicConnectionStringOnServer(TestBase):
    def setUp(self):
        super().setUp()

    def test_anthropic_settings_survive_a_connection_string_round_trip(self):
        connection_string = AiConnectionString(
            name="anthropic-round-trip",
            identifier="anthropic-round-trip",
            model_type=AiModelType.CHAT,
            anthropic_settings=AnthropicSettings(
                "api-key",
                _MODEL,
                max_output_tokens=2000,
                reasoning_effort="medium",
                enable_prompt_cache=True,
                api_version="2023-06-01",
            ),
        )

        self.store.maintenance.send(PutConnectionStringOperation(connection_string))

        result = self.store.maintenance.send(
            GetConnectionStringsOperation(connection_string.name, ConnectionStringType.AI)
        )
        retrieved = result.ai_connection_strings[connection_string.name]
        settings = retrieved.anthropic_settings

        self.assertEqual(AiConnectorType.ANTHROPIC, retrieved.get_active_provider())
        self.assertEqual("api-key", settings.api_key)
        self.assertEqual(_MODEL, settings.model)
        self.assertIsNone(settings.endpoint)
        self.assertEqual(2000, settings.max_output_tokens)
        self.assertEqual("medium", settings.reasoning_effort)
        self.assertTrue(settings.enable_prompt_cache)
        self.assertEqual("2023-06-01", settings.api_version)

    def test_put_with_text_embeddings_model_type_is_rejected_by_the_server(self):
        connection_string = AiConnectionString(
            name="anthropic-embeddings",
            identifier="anthropic-embeddings",
            model_type=AiModelType.CHAT,
            anthropic_settings=AnthropicSettings("api-key", _MODEL),
        )
        connection_string.model_type = AiModelType.TEXT_EMBEDDINGS

        self.assertRaisesWithMessageContaining(
            lambda: self.store.maintenance.send(PutConnectionStringOperation(connection_string)),
            Exception,
            "does not support the 'TextEmbeddings' model type",
        )


if __name__ == "__main__":
    unittest.main()
