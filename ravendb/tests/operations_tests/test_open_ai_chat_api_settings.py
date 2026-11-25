import unittest

from ravendb.documents.operations.ai.ai_connection_string import AiConnectionString, AiModelType
from ravendb.documents.operations.ai.azure_open_ai_settings import AzureOpenAiSettings
from ravendb.documents.operations.ai.ollama_settings import OllamaSettings
from ravendb.documents.operations.ai.open_ai_chat_api_settings import OpenAiChatApiSettings, OpenAiChatApiType
from ravendb.documents.operations.ai.open_ai_settings import OpenAiSettings
from ravendb.documents.operations.connection_string.get_connection_string_operation import GetConnectionStringsOperation
from ravendb.documents.operations.connection_string.put_connection_string_operation import (
    PutConnectionStringOperation,
)
from ravendb.serverwide.server_operation_executor import ConnectionStringType
from ravendb.tests.test_base import TestBase


def _openai(**kwargs) -> OpenAiSettings:
    return OpenAiSettings("api-key", "https://api.openai.com/", "gpt-5.4", **kwargs)


def _azure(**kwargs) -> AzureOpenAiSettings:
    return AzureOpenAiSettings("api-key", "https://test.openai.azure.com/", "gpt-5.4", "deployment", **kwargs)


def _ollama(**kwargs) -> OllamaSettings:
    return OllamaSettings("http://localhost:11434", "llama3.2", **kwargs)


class TestOpenAiChatApiSettingsSerialization(unittest.TestCase):
    def test_chat_api_and_reasoning_effort_are_written_when_set(self):
        for create in [_openai, _azure, _ollama]:
            with self.subTest(settings=create.__name__):
                json_dict = create(chat_api=OpenAiChatApiType.RESPONSES, reasoning_effort="low").to_json()

                self.assertEqual("Responses", json_dict["ChatApi"])
                self.assertEqual("low", json_dict["ReasoningEffort"])

    def test_chat_api_and_reasoning_effort_are_omitted_when_not_set(self):
        for create in [_openai, _azure, _ollama]:
            with self.subTest(settings=create.__name__):
                json_dict = create(reasoning_effort="  ").to_json()

                self.assertNotIn("ChatApi", json_dict)
                self.assertNotIn("ReasoningEffort", json_dict)

    def test_json_round_trip(self):
        for create in [_openai, _azure, _ollama]:
            with self.subTest(settings=create.__name__):
                settings = create(chat_api=OpenAiChatApiType.CHAT_COMPLETIONS, reasoning_effort="high")

                read = type(settings).from_json(settings.to_json())

                self.assertIsInstance(read, OpenAiChatApiSettings)
                self.assertEqual(OpenAiChatApiType.CHAT_COMPLETIONS, read.chat_api)
                self.assertEqual("high", read.reasoning_effort)

    def test_missing_chat_api_reads_as_not_set(self):
        self.assertIsNone(OpenAiSettings.from_json({"ApiKey": "api-key", "Model": "gpt-5.4"}).chat_api)
        self.assertIsNone(OllamaSettings.from_json({"Uri": "http://localhost:11434", "Model": "llama3.2"}).chat_api)


class TestOllamaSettingsAsOpenAiChatApiSettings(unittest.TestCase):
    def test_uri_is_the_endpoint_and_is_stored_as_uri(self):
        settings = _ollama()

        self.assertEqual("http://localhost:11434", settings.endpoint)
        self.assertEqual("http://localhost:11434", settings.to_json()["Uri"])
        self.assertNotIn("Endpoint", settings.to_json())

        settings.uri = "http://host/ollama"
        self.assertEqual("http://host/ollama", settings.endpoint)

    def test_api_key_is_optional(self):
        self.assertNotIn("ApiKey", _ollama().to_json())
        self.assertEqual("ollama-key", _ollama(api_key="ollama-key").to_json()["ApiKey"])
        self.assertEqual("ollama-key", OllamaSettings.from_json(_ollama(api_key="ollama-key").to_json()).api_key)

    def test_endpoint_is_read_when_uri_is_missing(self):
        settings = OllamaSettings.from_json({"Endpoint": "http://localhost:11434", "Model": "llama3.2"})

        self.assertEqual("http://localhost:11434", settings.uri)


class TestOpenAiChatApiSettingsConnectionString(TestBase):
    def setUp(self):
        super().setUp()

    def _round_trip(self, name: str, **settings) -> AiConnectionString:
        connection_string = AiConnectionString(name=name, identifier=name, model_type=AiModelType.CHAT, **settings)
        self.store.maintenance.send(PutConnectionStringOperation(connection_string))

        result = self.store.maintenance.send(GetConnectionStringsOperation(name, ConnectionStringType.AI))
        return result.ai_connection_strings[name]

    def test_chat_api_and_reasoning_effort_survive_a_connection_string_round_trip(self):
        openai = self._round_trip(
            "openai-chat-api",
            openai_settings=_openai(chat_api=OpenAiChatApiType.CHAT_COMPLETIONS, reasoning_effort="low"),
        ).openai_settings
        self.assertEqual(OpenAiChatApiType.CHAT_COMPLETIONS, openai.chat_api)
        self.assertEqual("low", openai.reasoning_effort)

        azure = self._round_trip(
            "azure-chat-api",
            azure_openai_settings=_azure(chat_api=OpenAiChatApiType.RESPONSES, reasoning_effort="medium"),
        ).azure_openai_settings
        self.assertEqual(OpenAiChatApiType.RESPONSES, azure.chat_api)
        self.assertEqual("medium", azure.reasoning_effort)

        ollama = self._round_trip(
            "ollama-chat-api",
            ollama_settings=_ollama(api_key="ollama-key", chat_api=OpenAiChatApiType.RESPONSES),
        ).ollama_settings
        self.assertEqual("http://localhost:11434", ollama.uri)
        self.assertEqual("ollama-key", ollama.api_key)
        self.assertEqual(OpenAiChatApiType.RESPONSES, ollama.chat_api)

    def test_settings_without_chat_api_keep_the_default(self):
        ollama = self._round_trip("ollama-default", ollama_settings=_ollama()).ollama_settings

        self.assertEqual("http://localhost:11434", ollama.uri)
        self.assertIsNone(ollama.api_key)
        self.assertIsNone(ollama.chat_api)


if __name__ == "__main__":
    unittest.main()
