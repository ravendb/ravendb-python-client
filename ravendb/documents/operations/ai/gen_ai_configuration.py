from __future__ import annotations
from typing import Dict, Any, Optional, List

from ravendb.documents.operations.ai.abstract_ai_integration_configuration import AbstractAiIntegrationConfiguration
from ravendb.documents.operations.ai.ai_task_identifier_helper import AiTaskIdentifierHelper
from ravendb.documents.operations.ai.gen_ai_transformation import GenAiTransformation
from ravendb.documents.operations.ai.agents.ai_agent_configuration import AiAgentToolQuery
from ravendb.documents.operations.etl.etl_type import EtlType
from ravendb.documents.operations.etl.transformation import Transformation


class GenAiConfiguration(AbstractAiIntegrationConfiguration):
    """
    Configuration for GenAI document processing in RavenDB.
    This configuration defines how documents from a collection are processed using AI.
    """

    DEFAULT_MAX_CONCURRENCY = 4
    TRANSFORMATION_NAME = "GenAi-transform-script"

    def __init__(
        self,
        name: str = None,
        identifier: str = None,
        collection: str = None,
        connection_string_name: str = None,
        prompt: str = None,
        json_schema: str = None,
        sample_object: str = None,
        update_script: str = None,
        gen_ai_transformation: GenAiTransformation = None,
        max_concurrency: int = None,
        queries: List[AiAgentToolQuery] = None,
        enable_tracing: bool = False,
        expiration_in_sec: int = None,
        version: int = None,
        disabled: bool = False,
        mentor_node: str = None,
        pin_to_mentor_node: bool = False,
        task_id: int = 0,
        allow_etl_on_non_encrypted_channel: bool = False,
    ):
        super().__init__(
            name=name,
            task_id=task_id,
            connection_string_name=connection_string_name,
            mentor_node=mentor_node,
            pin_to_mentor_node=pin_to_mentor_node,
            disabled=disabled,
            allow_etl_on_non_encrypted_channel=allow_etl_on_non_encrypted_channel,
        )

        self.identifier = identifier
        self.collection = collection
        self.prompt = prompt
        self.json_schema = json_schema
        self.sample_object = sample_object
        self.update_script = update_script
        self.gen_ai_transformation = gen_ai_transformation
        self.max_concurrency = max_concurrency if max_concurrency is not None else self.DEFAULT_MAX_CONCURRENCY
        self.queries: List[AiAgentToolQuery] = queries or []
        self.enable_tracing = enable_tracing
        self.expiration_in_sec: Optional[int] = expiration_in_sec
        self.version: Optional[int] = version

        self._transforms: Optional[List[Transformation]] = None

    @property
    def etl_type(self) -> EtlType:
        return EtlType.GEN_AI

    def get_destination(self) -> str:
        """Returns the destination identifier for this configuration."""
        return self.identifier

    def get_default_task_name(self) -> str:
        """Returns the default task name for this configuration."""
        return self.identifier

    def using_encrypted_communication_channel(self) -> bool:
        """Returns True if the connection uses encrypted communication."""
        if self.connection:
            return self.connection.using_encrypted_communication_channel()
        return False

    # todo: use validate in __init__ or in some more suitable place
    def validate(
        self,
        validate_name: bool = True,
        # todo: validate_connection: bool = True,
        validate_identifier: bool = True,
    ) -> List[str]:
        """
        Validates the GenAI configuration.

        Args:
            validate_name: Whether to validate the name field.
            validate_identifier: Whether to validate the identifier format.

        Returns:
            A list of validation error messages. Empty list if valid.
        """
        errors: List[str] = []

        # Validate identifier using AiTaskIdentifierHelper
        if validate_identifier:
            is_valid, id_errors = AiTaskIdentifierHelper.validate_identifier(self.identifier)
            if not is_valid:
                errors.extend(id_errors)

        if validate_name and not self.name:
            errors.append("Name of GenAi configuration cannot be empty")

        if not self._test_mode and not self.connection_string_name:
            errors.append("ConnectionStringName cannot be empty")

        if not self.collection:
            errors.append("Collection must be provided")

        if self.gen_ai_transformation is None:
            errors.append("GenAiTransformation must be specified")
        else:
            is_valid, error = self.gen_ai_transformation.validate_script()
            if not is_valid:
                errors.append(error)

        if not self._test_mode:
            if not self.prompt:
                errors.append("Prompt must be provided")

            if not self.json_schema and not self.sample_object:
                errors.append("You must provide either a JSON schema or a sample object")

            if not self.update_script:
                errors.append("You must provide an update function")

        return errors

    def generate_identifier(self) -> str:
        """Generates a valid identifier based on the configuration name."""
        return AiTaskIdentifierHelper.generate_identifier(self.name)

    @property
    def transforms(self) -> List[Transformation]:
        """
        GenAiConfiguration uses a single transformation based on GenAiTransformation.
        This property is provided for compatibility with the base EtlConfiguration.
        """
        if self._transforms is None:
            self._transforms = [
                Transformation(
                    name=self.TRANSFORMATION_NAME,
                    collections=[self.collection] if self.collection else [],
                    script=self.gen_ai_transformation.script if self.gen_ai_transformation else None,
                )
            ]
        return self._transforms

    @transforms.setter
    def transforms(self, value: List[Transformation]):
        raise NotImplementedError(
            "GenAiConfiguration doesn't support multiple transformations. "
            "Please use gen_ai_transformation property instead."
        )

    def to_json(self) -> Dict[str, Any]:
        result = super().to_json()
        result.update(
            {
                "Identifier": self.identifier,
                "AiConnectorType": self.ai_connector_type.value if self.ai_connector_type else None,
                "Collection": self.collection,
                "Prompt": self.prompt,
                "JsonSchema": self.json_schema,
                "SampleObject": self.sample_object,
                "UpdateScript": self.update_script,
                "GenAiTransformation": self.gen_ai_transformation.to_json() if self.gen_ai_transformation else None,
                "MaxConcurrency": self.max_concurrency,
                "Queries": [q.to_json() for q in self.queries] if self.queries else None,
                "EnableTracing": self.enable_tracing,
                "ExpirationInSec": self.expiration_in_sec,
            }
        )
        if self.version is not None:
            result["Version"] = self.version
        return result

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "GenAiConfiguration":
        transformation_data = json_dict.get("GenAiTransformation")
        queries_data = json_dict.get("Queries")

        return cls(
            name=json_dict.get("Name"),
            task_id=json_dict.get("TaskId", 0),
            identifier=json_dict.get("Identifier"),
            collection=json_dict.get("Collection"),
            connection_string_name=json_dict.get("ConnectionStringName"),
            prompt=json_dict.get("Prompt"),
            json_schema=json_dict.get("JsonSchema"),
            sample_object=json_dict.get("SampleObject"),
            update_script=json_dict.get("UpdateScript"),
            gen_ai_transformation=GenAiTransformation.from_json(transformation_data) if transformation_data else None,
            max_concurrency=json_dict.get("MaxConcurrency", cls.DEFAULT_MAX_CONCURRENCY),
            queries=[AiAgentToolQuery.from_json(q) for q in queries_data] if queries_data else None,
            enable_tracing=json_dict.get("EnableTracing", False),
            expiration_in_sec=json_dict.get("ExpirationInSec"),
            version=json_dict.get("Version"),
            disabled=json_dict.get("Disabled", False),
            mentor_node=json_dict.get("MentorNode"),
            pin_to_mentor_node=json_dict.get("PinToMentorNode", False),
            allow_etl_on_non_encrypted_channel=json_dict.get("AllowEtlOnNonEncryptedChannel", False),
        )
