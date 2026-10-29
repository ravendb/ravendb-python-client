import unittest

from ravendb.documents.operations.ai.ai_connection_string import AiConnectionString, AiModelType
from ravendb.documents.operations.ai.azure_open_ai_settings import AzureOpenAiSettings
from ravendb.documents.operations.ai.google_settings import GoogleAiVersion, GoogleSettings
from ravendb.documents.operations.ai.open_ai_settings import OpenAiSettings
from ravendb.documents.operations.connection_string.get_connection_string_operation import GetConnectionStringsOperation
from ravendb.documents.operations.connection_string.put_connection_string_operation import (
    PutConnectionStringOperation,
)
from ravendb.serverwide.server_operation_executor import ConnectionStringType
from ravendb.tests.test_base import TestBase

_ENDPOINT = "https://api.example.com/"


def _settings(enable_prompt_cache):
    return {
        "openai_settings": OpenAiSettings("api-key", _ENDPOINT, "gpt-5-mini", enable_prompt_cache=enable_prompt_cache),
        "azure_openai_settings": AzureOpenAiSettings(
            "api-key", _ENDPOINT, "gpt-5-mini", "deployment", enable_prompt_cache=enable_prompt_cache
        ),
        "google_settings": GoogleSettings(
            "gemini-2.5-flash", "api-key", GoogleAiVersion.V1, enable_prompt_cache=enable_prompt_cache
        ),
    }


class TestEnablePromptCacheSerialization(unittest.TestCase):
    def test_enable_prompt_cache_is_written_when_set(self):
        for value in [True, False]:
            for name, settings in _settings(value).items():
                with self.subTest(settings=name, value=value):
                    self.assertIs(value, settings.to_json()["EnablePromptCache"])

    def test_enable_prompt_cache_is_omitted_when_not_set(self):
        for name, settings in _settings(None).items():
            with self.subTest(settings=name):
                self.assertNotIn("EnablePromptCache", settings.to_json())

    def test_json_round_trip(self):
        for value in [True, False, None]:
            for name, settings in _settings(value).items():
                with self.subTest(settings=name, value=value):
                    self.assertIs(value, type(settings).from_json(settings.to_json()).enable_prompt_cache)

    def test_missing_enable_prompt_cache_reads_as_not_set(self):
        for name, settings in _settings(True).items():
            with self.subTest(settings=name):
                json_dict = settings.to_json()
                del json_dict["EnablePromptCache"]
                self.assertIsNone(type(settings).from_json(json_dict).enable_prompt_cache)


class TestEnablePromptCacheConnectionString(TestBase):
    def setUp(self):
        super().setUp()

    def test_enable_prompt_cache_survives_a_connection_string_round_trip(self):
        for value in [True, False, None]:
            for name, settings in _settings(value).items():
                with self.subTest(settings=name, value=value):
                    connection_string_name = "prompt-cache-" + name.replace("_", "-")
                    connection_string = AiConnectionString(
                        name=connection_string_name,
                        identifier=connection_string_name,
                        model_type=AiModelType.CHAT,
                        **{name: settings},
                    )

                    self.store.maintenance.send(PutConnectionStringOperation(connection_string))

                    result = self.store.maintenance.send(
                        GetConnectionStringsOperation(connection_string.name, ConnectionStringType.AI)
                    )
                    retrieved = getattr(result.ai_connection_strings[connection_string.name], name)

                    self.assertIs(value, retrieved.enable_prompt_cache)


if __name__ == "__main__":
    unittest.main()
