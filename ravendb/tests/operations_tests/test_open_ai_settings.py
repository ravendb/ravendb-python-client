import unittest

from ravendb.documents.operations.ai.ai_connection_string import AiConnectionString
from ravendb.documents.operations.ai.open_ai_settings import OpenAiSettings
from ravendb.serverwide.operations.connection_strings import GetServerWideConnectionStringsResult

# What the server returns for a server-wide AI connection string put without an endpoint:
# null fields are left out of the settings object instead of being sent as null.
_SERVER_WIDE_RESPONSE = {
    "Results": [
        {
            "Name": "My Server Wide AI CS",
            "Identifier": "my-server-wide-ai-cs",
            "ModelType": "Chat",
            "OpenAiSettings": {"EmbeddingsMaxConcurrentBatches": None, "Model": "test", "ApiKey": "fake-key"},
            "AzureOpenAiSettings": None,
            "OllamaSettings": None,
            "EmbeddedSettings": None,
            "GoogleSettings": None,
            "HuggingFaceSettings": None,
            "MistralAiSettings": None,
            "VertexSettings": None,
            "AnthropicSettings": None,
            "Type": "Ai",
            "ExcludedDatabases": None,
            "UsedBy": [],
        }
    ]
}


class TestOpenAiSettingsFromJson(unittest.TestCase):
    def test_omitted_fields_read_as_not_set(self):
        settings = OpenAiSettings.from_json({"ApiKey": "fake-key", "Model": "test"})

        self.assertEqual("fake-key", settings.api_key)
        self.assertEqual("test", settings.model)
        self.assertIsNone(settings.endpoint)
        self.assertIsNone(settings.temperature)

    def test_server_wide_connection_string_without_endpoint_is_read(self):
        result = GetServerWideConnectionStringsResult.from_json(_SERVER_WIDE_RESPONSE)

        self.assertEqual(1, len(result.results))
        connection_string = result.results[0].connection_string
        self.assertIsInstance(connection_string, AiConnectionString)
        self.assertEqual("my-server-wide-ai-cs", connection_string.identifier)
        self.assertEqual("fake-key", connection_string.openai_settings.api_key)
        self.assertIsNone(connection_string.openai_settings.endpoint)


if __name__ == "__main__":
    unittest.main()
