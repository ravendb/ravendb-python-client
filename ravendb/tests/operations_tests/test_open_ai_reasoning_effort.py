import unittest

from ravendb.documents.operations.ai.ai_connection_string import AiConnectionString, AiModelType
from ravendb.documents.operations.ai.open_ai_settings import OpenAiSettings
from ravendb.documents.operations.connection_string.get_connection_string_operation import GetConnectionStringsOperation
from ravendb.documents.operations.connection_string.put_connection_string_operation import (
    PutConnectionStringOperation,
)
from ravendb.serverwide.server_operation_executor import ConnectionStringType
from ravendb.tests.test_base import TestBase

_ENDPOINT = "https://api.openai.com/"


class TestOpenAiReasoningEffortSerialization(unittest.TestCase):
    def test_reasoning_effort_is_written_when_set(self):
        for effort in ["high", "xhigh", "none", "High", "future-effort"]:
            with self.subTest(effort=effort):
                settings = OpenAiSettings("api-key", _ENDPOINT, "gpt-5-mini", reasoning_effort=effort)
                self.assertEqual(effort, settings.to_json()["ReasoningEffort"])

    def test_reasoning_effort_is_omitted_when_not_set_or_blank(self):
        for effort in [None, "", "   "]:
            with self.subTest(effort=effort):
                settings = OpenAiSettings("api-key", _ENDPOINT, "gpt-5-mini", reasoning_effort=effort)
                self.assertNotIn("ReasoningEffort", settings.to_json())

    def test_reasoning_effort_is_read_from_json(self):
        json_dict = {"ApiKey": "api-key", "Endpoint": _ENDPOINT, "Model": "gpt-5-mini", "ReasoningEffort": "High"}
        self.assertEqual("High", OpenAiSettings.from_json(json_dict).reasoning_effort)

    def test_missing_or_numeric_reasoning_effort_reads_as_not_configured(self):
        json_dict = {"ApiKey": "api-key", "Endpoint": _ENDPOINT, "Model": "gpt-5-mini"}
        self.assertIsNone(OpenAiSettings.from_json(json_dict).reasoning_effort)

        json_dict["ReasoningEffort"] = 3
        self.assertIsNone(OpenAiSettings.from_json(json_dict).reasoning_effort)

    def test_json_round_trip(self):
        settings = OpenAiSettings("api-key", _ENDPOINT, "gpt-5.2", reasoning_effort="xhigh")
        self.assertEqual("xhigh", OpenAiSettings.from_json(settings.to_json()).reasoning_effort)


class TestOpenAiReasoningEffortConnectionString(TestBase):
    def setUp(self):
        super().setUp()

    def test_reasoning_effort_survives_a_connection_string_round_trip(self):
        # "High" is also the shape persisted by earlier enum-based versions
        for effort in ["High", "xhigh", None]:
            with self.subTest(effort=effort):
                connection_string = AiConnectionString(
                    name="reasoning-effort-round-trip",
                    identifier="reasoning-effort-round-trip",
                    model_type=AiModelType.CHAT,
                    openai_settings=OpenAiSettings("api-key", _ENDPOINT, "gpt-5-mini", reasoning_effort=effort),
                )

                self.store.maintenance.send(PutConnectionStringOperation(connection_string))

                result = self.store.maintenance.send(
                    GetConnectionStringsOperation(connection_string.name, ConnectionStringType.AI)
                )
                settings = result.ai_connection_strings[connection_string.name].openai_settings

                self.assertEqual(effort, settings.reasoning_effort)


if __name__ == "__main__":
    unittest.main()
