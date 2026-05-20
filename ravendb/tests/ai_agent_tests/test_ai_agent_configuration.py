"""
Unit tests for the 7.2.3 AI agent configuration additions:
  * AiAgentParameter.policy / .type enums
  * AiAgentToolSubAgent
  * AiAgentConfiguration.sub_agents
  * MissingAiAgentParameterException + dispatcher mapping
  * AiMessagePromptFields.IMAGE
  * AzureOpenAiSettings endpoint validation
"""

import unittest

from ravendb import (
    AiAgentConfiguration,
    AiAgentParameter,
    AiAgentParameterPolicy,
    AiAgentParameterValueType,
    AiAgentToolSubAgent,
)
from ravendb.documents.ai.content_part import AiMessagePromptFields
from ravendb.documents.operations.ai.azure_open_ai_settings import AzureOpenAiSettings
from ravendb.exceptions.exception_dispatcher import _EXCEPTION_MAP
from ravendb.exceptions.raven_exceptions import MissingAiAgentParameterException


class TestAiAgentParameterPolicyAndType(unittest.TestCase):
    def test_defaults(self):
        p = AiAgentParameter(name="userId")
        self.assertEqual(AiAgentParameterPolicy.DEFAULT, p.policy)
        self.assertEqual(AiAgentParameterValueType.DEFAULT, p.type)

    def test_to_json_emits_policy_and_type(self):
        p = AiAgentParameter(
            name="userId",
            description="Hidden",
            send_to_model=False,
            policy=AiAgentParameterPolicy.FORBID_MODEL_GENERATION,
            type=AiAgentParameterValueType.STRING,
        )
        out = p.to_json()
        self.assertEqual(1, out["Policy"])
        self.assertEqual("String", out["Type"])

    def test_from_json_round_trip(self):
        out = {
            "Name": "u",
            "Description": "d",
            "SendToModel": True,
            "Policy": 1,
            "Type": "ArrayOfNumber",
        }
        p = AiAgentParameter.from_json(out)
        self.assertEqual(AiAgentParameterPolicy.FORBID_MODEL_GENERATION, p.policy)
        self.assertEqual(AiAgentParameterValueType.ARRAY_OF_NUMBER, p.type)

    def test_from_json_accepts_policy_as_string(self):
        # The server returns Policy as the enum *name* string in GET responses
        # (e.g. "Default", "ForbidModelGeneration"), even though Python emits
        # the int on the way out. from_json must handle both forms.
        p = AiAgentParameter.from_json({"Name": "u", "Policy": "ForbidModelGeneration"})
        self.assertEqual(AiAgentParameterPolicy.FORBID_MODEL_GENERATION, p.policy)

    def test_from_json_default_policy_as_string(self):
        p = AiAgentParameter.from_json({"Name": "u", "Policy": "Default"})
        self.assertEqual(AiAgentParameterPolicy.DEFAULT, p.policy)


class TestAiAgentToolSubAgent(unittest.TestCase):
    def test_round_trip(self):
        sa = AiAgentToolSubAgent(identifier="benefits-agent", description="Handles benefits questions")
        out = sa.to_json()
        self.assertEqual("benefits-agent", out["Identifier"])
        self.assertEqual("Handles benefits questions", out["Description"])
        back = AiAgentToolSubAgent.from_json(out)
        self.assertEqual(sa.identifier, back.identifier)
        self.assertEqual(sa.description, back.description)


class TestAiAgentConfigurationSubAgents(unittest.TestCase):
    def test_to_json_includes_sub_agents(self):
        cfg = AiAgentConfiguration(
            name="main",
            connection_string_name="cs",
            system_prompt="be helpful",
            sub_agents=[
                AiAgentToolSubAgent("benefits", "benefits-related"),
                AiAgentToolSubAgent("attendance", "PTO tracking"),
            ],
        )
        out = cfg.to_json()
        self.assertEqual(2, len(out["SubAgents"]))
        self.assertEqual("benefits", out["SubAgents"][0]["Identifier"])


class TestMissingAiAgentParameterException(unittest.TestCase):
    def test_dispatcher_registers_short_name(self):
        self.assertIn("MissingAiAgentParameterException", _EXCEPTION_MAP)
        self.assertIs(MissingAiAgentParameterException, _EXCEPTION_MAP["MissingAiAgentParameterException"])

    def test_is_raven_exception(self):
        from ravendb.exceptions.raven_exceptions import RavenException

        self.assertTrue(issubclass(MissingAiAgentParameterException, RavenException))


class TestAiMessagePromptFieldsImage(unittest.TestCase):
    def test_image_constant(self):
        self.assertEqual("image", AiMessagePromptFields.IMAGE)


class TestAzureOpenAiSettingsEndpointValidation(unittest.TestCase):
    def test_none_endpoint_rejected(self):
        with self.assertRaises(ValueError):
            AzureOpenAiSettings(api_key="k", endpoint=None, model="m", deployment_name="d")

    def test_blank_endpoint_rejected(self):
        with self.assertRaises(ValueError):
            AzureOpenAiSettings(api_key="k", endpoint="   ", model="m", deployment_name="d")

    def test_valid_endpoint_accepted(self):
        s = AzureOpenAiSettings(api_key="k", endpoint="https://x.openai.azure.com", model="m", deployment_name="d")
        self.assertEqual("https://x.openai.azure.com", s.endpoint)


if __name__ == "__main__":
    unittest.main()
