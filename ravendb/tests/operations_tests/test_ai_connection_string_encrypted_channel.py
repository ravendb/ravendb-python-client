import unittest

from ravendb.documents.operations.ai.ai_connection_string import AiConnectionString, AiModelType
from ravendb.documents.operations.ai.anthropic_settings import AnthropicSettings
from ravendb.documents.operations.ai.azure_open_ai_settings import AzureOpenAiSettings
from ravendb.documents.operations.ai.embedded_settings import EmbeddedSettings
from ravendb.documents.operations.ai.embeddings_generation_configuration import EmbeddingsGenerationConfiguration
from ravendb.documents.operations.ai.gen_ai_configuration import GenAiConfiguration
from ravendb.documents.operations.ai.google_settings import GoogleAiVersion, GoogleSettings
from ravendb.documents.operations.ai.hugging_face_settings import HuggingFaceSettings
from ravendb.documents.operations.ai.mistral_ai_settings import MistralAiSettings
from ravendb.documents.operations.ai.ollama_settings import OllamaSettings
from ravendb.documents.operations.ai.open_ai_settings import OpenAiSettings
from ravendb.documents.operations.ai.vertex_settings import VertexAIVersion, VertexSettings


def _connection_string(model_type: AiModelType = AiModelType.TEXT_EMBEDDINGS, **settings) -> AiConnectionString:
    return AiConnectionString(
        name="encrypted-channel", identifier="encrypted-channel", model_type=model_type, **settings
    )


class TestAiConnectionStringEncryptedChannel(unittest.TestCase):
    def test_ollama_checks_the_uri(self):
        for uri, expected in [("https://ollama.example.com", True), ("http://localhost:11434", False)]:
            with self.subTest(uri=uri):
                connection_string = _connection_string(ollama_settings=OllamaSettings(uri, "llama3"))
                self.assertEqual(expected, connection_string.using_encrypted_communication_channel())

    def test_endpoint_providers_check_the_endpoint(self):
        for endpoint, expected in [
            ("https://api.example.com/", True),
            ("HTTPS://api.example.com/", True),
            ("http://localhost:8080/", False),
        ]:
            for settings in [
                {"openai_settings": OpenAiSettings("api-key", endpoint, "text-embedding-3-small")},
                {"azure_openai_settings": AzureOpenAiSettings("api-key", endpoint, "model", "deployment")},
                {"mistral_ai_settings": MistralAiSettings("api-key", "mistral-embed", endpoint)},
            ]:
                with self.subTest(endpoint=endpoint, settings=list(settings)[0]):
                    connection_string = _connection_string(**settings)
                    self.assertEqual(expected, connection_string.using_encrypted_communication_channel())

    def test_hugging_face_without_endpoint_uses_the_https_default(self):
        for endpoint, expected in [
            (None, True),
            ("", True),
            ("   ", True),
            ("https://hf.example.com", True),
            ("http://localhost:8080", False),
        ]:
            with self.subTest(endpoint=endpoint):
                connection_string = _connection_string(
                    huggingface_settings=HuggingFaceSettings("api-key", "sentence-transformers/model", endpoint)
                )
                self.assertIs(expected, connection_string.using_encrypted_communication_channel())

    def test_anthropic_without_endpoint_uses_the_https_default(self):
        for endpoint, expected in [
            (None, True),
            ("", True),
            ("   ", True),
            ("https://anthropic.example.com/v1/", True),
            ("http://localhost:8080/", False),
        ]:
            with self.subTest(endpoint=endpoint):
                connection_string = _connection_string(
                    model_type=AiModelType.CHAT,
                    anthropic_settings=AnthropicSettings("api-key", "claude-model", endpoint),
                )
                self.assertIs(expected, connection_string.using_encrypted_communication_channel())

    def test_providers_without_an_endpoint_are_encrypted(self):
        for settings in [
            {"embedded_settings": EmbeddedSettings()},
            {"google_settings": GoogleSettings("text-embedding-004", "api-key", GoogleAiVersion.V1)},
            {"vertex_settings": VertexSettings("text-embedding-005", "{}", "us-central1", VertexAIVersion.V1)},
        ]:
            with self.subTest(settings=list(settings)[0]):
                self.assertTrue(_connection_string(**settings).using_encrypted_communication_channel())

    def test_ai_task_configurations_report_the_connection_string_value(self):
        connection_string = _connection_string(
            model_type=AiModelType.CHAT, ollama_settings=OllamaSettings("https://ollama.example.com", "llama3")
        )

        for configuration in [
            GenAiConfiguration(name="gen-ai", identifier="gen-ai", connection_string_name=connection_string.name),
            EmbeddingsGenerationConfiguration(
                name="embeddings", identifier="embeddings", connection_string_name=connection_string.name
            ),
        ]:
            with self.subTest(configuration=type(configuration).__name__):
                self.assertFalse(configuration.using_encrypted_communication_channel())
                configuration.initialize(connection_string)
                self.assertTrue(configuration.using_encrypted_communication_channel())


if __name__ == "__main__":
    unittest.main()
