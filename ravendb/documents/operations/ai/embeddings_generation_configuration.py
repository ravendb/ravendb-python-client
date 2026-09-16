from __future__ import annotations
from datetime import timedelta
from typing import Dict, Any, Optional, List

from ravendb.documents.indexes.vector.embedding import VectorEmbeddingType
from ravendb.documents.operations.ai.abstract_ai_integration_configuration import AbstractAiIntegrationConfiguration
from ravendb.documents.operations.ai.ai_task_identifier_helper import AiTaskIdentifierHelper
from ravendb.documents.operations.ai.chunking_options import ChunkingOptions
from ravendb.documents.operations.ai.embedding_path_configuration import EmbeddingPathConfiguration
from ravendb.documents.operations.ai.embeddings_transformation import EmbeddingsTransformation
from ravendb.documents.operations.etl.etl_type import EtlType
from ravendb.documents.operations.etl.transformation import Transformation
from ravendb.tools.utils import Utils


class EmbeddingsGenerationConfiguration(AbstractAiIntegrationConfiguration):
    """
    Configuration for Embeddings Generation tasks in RavenDB.
    This configuration defines how documents from a collection are processed to generate embeddings.
    """

    PATHS_TRANSFORMATION_NAME = "embeddings-from-paths"
    SCRIPT_TRANSFORMATION_NAME = "embeddings-transform-script"

    DEFAULT_EMBEDDINGS_CACHE_EXPIRATION = timedelta(days=90)
    DEFAULT_EMBEDDINGS_CACHE_FOR_QUERYING_EXPIRATION = timedelta(days=14)

    def __init__(
        self,
        name: str = None,
        identifier: str = None,
        collection: str = None,
        connection_string_name: str = None,
        embeddings_path_configurations: List[EmbeddingPathConfiguration] = None,
        embeddings_transformation: EmbeddingsTransformation = None,
        quantization: VectorEmbeddingType = None,
        chunking_options_for_querying: ChunkingOptions = None,
        embeddings_cache_expiration: timedelta = None,
        embeddings_cache_for_querying_expiration: timedelta = None,
        store_chunk_text: bool = False,
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
        self.embeddings_path_configurations = embeddings_path_configurations
        self.embeddings_transformation = embeddings_transformation
        self.quantization = quantization
        self.chunking_options_for_querying = chunking_options_for_querying
        # Keeps each chunk's text next to its embedding, which helps when debugging or
        # highlighting but costs storage. Off by default.
        self.store_chunk_text = store_chunk_text
        self.embeddings_cache_expiration = (
            embeddings_cache_expiration
            if embeddings_cache_expiration is not None
            else self.DEFAULT_EMBEDDINGS_CACHE_EXPIRATION
        )
        self.embeddings_cache_for_querying_expiration = (
            embeddings_cache_for_querying_expiration
            if embeddings_cache_for_querying_expiration is not None
            else self.DEFAULT_EMBEDDINGS_CACHE_FOR_QUERYING_EXPIRATION
        )

        self._transforms: Optional[List[Transformation]] = None

    @property
    def etl_type(self) -> EtlType:
        return EtlType.EMBEDDINGS_GENERATION

    @property
    def transformation_name(self) -> str:
        """Returns the transformation name based on whether script or paths are used."""
        if self.embeddings_transformation is not None:
            return self.SCRIPT_TRANSFORMATION_NAME
        return self.PATHS_TRANSFORMATION_NAME

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

    def validate(
        self,
        validate_name: bool = True,
        validate_identifier: bool = True,
    ) -> List[str]:
        """
        Validates the EmbeddingsGeneration configuration.

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
            errors.append("Name of EmbeddingsGeneration configuration cannot be empty")

        if not self._test_mode and not self.connection_string_name:
            errors.append("ConnectionStringName cannot be empty")

        if not self.collection:
            errors.append("Collection must be provided")

        # Validate that either paths or transformation is provided, but not both
        has_paths = self.embeddings_path_configurations and len(self.embeddings_path_configurations) > 0
        has_transformation = self.embeddings_transformation is not None

        if not has_paths and not has_transformation:
            errors.append("Either EmbeddingsPathConfigurations or EmbeddingsTransformation must be provided")

        # Validate each path's chunking options (mirrors the server / C# client; both may be set).
        if self.embeddings_path_configurations:
            for path_configuration in self.embeddings_path_configurations:
                if path_configuration.chunking_options is not None:
                    path_configuration.chunking_options.validate(path_configuration.path, errors)
                else:
                    errors.append(f"Path '{path_configuration.path}': ChunkingOptions must be provided.")

        # Validate transformation if provided
        if has_transformation:
            self.embeddings_transformation.validate(errors)

        # Validate quantization
        if self.quantization == VectorEmbeddingType.TEXT:
            errors.append("Quantization cannot be set to Text for embeddings generation")

        # Validate chunking options for querying
        if not self.chunking_options_for_querying:
            errors.append("ChunkingOptionsForQuerying must be provided")
        else:
            self.chunking_options_for_querying.validate("ChunkingOptionsForQuerying", errors)

        return errors

    def generate_identifier(self) -> str:
        """Generates a valid identifier based on the configuration name."""
        return AiTaskIdentifierHelper.generate_identifier(self.name)

    @property
    def transforms(self) -> List[Transformation]:
        """
        EmbeddingsGenerationConfiguration uses a single transformation.
        This property is provided for compatibility with the base EtlConfiguration.
        """
        if self._transforms is None:
            script = None
            if self.embeddings_transformation:
                script = self.embeddings_transformation.script

            self._transforms = [
                Transformation(
                    name=self.transformation_name,
                    collections=[self.collection] if self.collection else [],
                    script=script,
                )
            ]
        return self._transforms

    @transforms.setter
    def transforms(self, value: List[Transformation]):
        raise NotImplementedError(
            "EmbeddingsGenerationConfiguration doesn't support multiple transformations. "
            "Please use embeddings_transformation or embeddings_path_configurations instead."
        )

    def to_json(self) -> Dict[str, Any]:
        result = super().to_json()
        result.update(
            {
                "Identifier": self.identifier,
                "AiConnectorType": self.ai_connector_type.value if self.ai_connector_type else None,
                "Collection": self.collection,
                "EmbeddingsPathConfigurations": (
                    [p.to_json() for p in self.embeddings_path_configurations]
                    if self.embeddings_path_configurations
                    else None
                ),
                "EmbeddingsTransformation": (
                    self.embeddings_transformation.to_json() if self.embeddings_transformation else None
                ),
                "Quantization": self.quantization.value if self.quantization else None,
                "ChunkingOptionsForQuerying": (
                    self.chunking_options_for_querying.to_json() if self.chunking_options_for_querying else None
                ),
                "EmbeddingsCacheExpiration": (
                    Utils.timedelta_to_str(self.embeddings_cache_expiration)
                    if self.embeddings_cache_expiration
                    else None
                ),
                "EmbeddingsCacheForQueryingExpiration": (
                    Utils.timedelta_to_str(self.embeddings_cache_for_querying_expiration)
                    if self.embeddings_cache_for_querying_expiration
                    else None
                ),
                "StoreChunkText": self.store_chunk_text,
            }
        )
        return result

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "EmbeddingsGenerationConfiguration":
        paths_data = json_dict.get("EmbeddingsPathConfigurations", None)
        transformation_data = json_dict.get("EmbeddingsTransformation", None)
        chunking_data = json_dict["ChunkingOptionsForQuerying"]
        quantization_data = json_dict.get("Quantization", None)
        cache_expiration = json_dict.get("EmbeddingsCacheExpiration", None)
        cache_for_querying_expiration = json_dict.get("EmbeddingsCacheForQueryingExpiration", None)

        return cls(
            name=json_dict["Name"],
            task_id=json_dict.get("TaskId", 0),
            identifier=json_dict["Identifier"],
            collection=json_dict["Collection"],
            connection_string_name=json_dict["ConnectionStringName"],
            embeddings_path_configurations=(
                [EmbeddingPathConfiguration.from_json(p) for p in paths_data] if paths_data else None
            ),
            embeddings_transformation=(
                EmbeddingsTransformation.from_json(transformation_data) if transformation_data else None
            ),
            quantization=VectorEmbeddingType(quantization_data) if quantization_data else None,
            chunking_options_for_querying=ChunkingOptions.from_json(chunking_data),
            embeddings_cache_expiration=(Utils.string_to_timedelta(cache_expiration) if cache_expiration else None),
            embeddings_cache_for_querying_expiration=(
                Utils.string_to_timedelta(cache_for_querying_expiration) if cache_for_querying_expiration else None
            ),
            disabled=json_dict.get("Disabled", False),
            mentor_node=json_dict.get("MentorNode", None),
            pin_to_mentor_node=json_dict.get("PinToMentorNode", False),
            allow_etl_on_non_encrypted_channel=json_dict.get("AllowEtlOnNonEncryptedChannel", False),
            store_chunk_text=json_dict.get("StoreChunkText", False),
        )
