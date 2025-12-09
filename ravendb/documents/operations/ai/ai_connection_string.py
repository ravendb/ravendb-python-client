import enum
from typing import Optional, Dict, Any

from ravendb.serverwide.server_operation_executor import ConnectionStringType
from ravendb.documents.operations.ai.azure_open_ai_settings import AzureOpenAiSettings
from ravendb.documents.operations.ai.embedded_settings import EmbeddedSettings
from ravendb.documents.operations.ai.google_settings import GoogleSettings
from ravendb.documents.operations.ai.hugging_face_settings import HuggingFaceSettings
from ravendb.documents.operations.ai.mistral_ai_settings import MistralAiSettings
from ravendb.documents.operations.ai.ollama_settings import OllamaSettings
from ravendb.documents.operations.ai.open_ai_settings import OpenAiSettings
from ravendb.documents.operations.ai.vertex_settings import VertexSettings

from ravendb.documents.operations.connection_strings import ConnectionString


class AiModelType(enum.Enum):
    TEXT_EMBEDDINGS = "TextEmbeddings"
    CHAT = "Chat"


class AiConnectorType(enum.Enum):
    NONE = "None"
    OPEN_AI = "OpenAi"
    AZURE_OPEN_AI = "AzureOpenAi"
    OLLAMA = "Ollama"
    EMBEDDED = "Embedded"
    GOOGLE = "Google"
    HUGGING_FACE = "HuggingFace"
    MISTRAL_AI = "MistralAi"
    VERTEX = "Vertex"


class AiConnectionString(ConnectionString):
    def __init__(
        self,
        name: str,
        identifier: str,
        openai_settings: Optional[OpenAiSettings] = None,
        azure_openai_settings: Optional[AzureOpenAiSettings] = None,
        ollama_settings: Optional[OllamaSettings] = None,
        embedded_settings: Optional[EmbeddedSettings] = None,
        google_settings: Optional[GoogleSettings] = None,
        huggingface_settings: Optional[HuggingFaceSettings] = None,
        mistral_ai_settings: Optional[MistralAiSettings] = None,
        vertex_settings: Optional[VertexSettings] = None,
        model_type: AiModelType = None,
    ):
        super().__init__(name)
        self.identifier = identifier
        self.openai_settings = openai_settings
        self.azure_openai_settings = azure_openai_settings
        self.ollama_settings = ollama_settings
        self.embedded_settings = embedded_settings
        self.google_settings = google_settings
        self.huggingface_settings = huggingface_settings
        self.mistral_ai_settings = mistral_ai_settings
        self.vertex_settings = vertex_settings
        self.model_type = model_type

        if not any(
            [
                openai_settings,
                azure_openai_settings,
                ollama_settings,
                embedded_settings,
                google_settings,
                huggingface_settings,
                mistral_ai_settings,
                vertex_settings,
            ]
        ):
            raise ValueError(
                "Please provide at least one of the following settings: openai_settings, azure_openai_settings, ollama_settings, embedded_settings, google_settings, huggingface_settings, mistral_ai_settings, vertex_settings"
            )

        if model_type is None:
            raise ValueError("Please provide a model type - AiModelType.TEXT_EMBEDDINGS or AiModelType.CHAT")

        settings_set_count = 0
        for setting in [
            openai_settings,
            azure_openai_settings,
            ollama_settings,
            embedded_settings,
            google_settings,
            huggingface_settings,
            mistral_ai_settings,
            vertex_settings,
        ]:
            if setting:
                settings_set_count += 1 if setting else 0
            if settings_set_count > 1:
                raise ValueError(
                    "Please provide only one of the following settings: openai_settings, azure_openai_settings, ollama_settings, embedded_settings, google_settings, huggingface_settings, mistral_ai_settings, vertex_settings"
                )

    @property
    def get_type(self):
        return ConnectionStringType.AI.value

    def get_active_provider(self) -> AiConnectorType:
        """Returns the active AI connector type based on which settings are configured."""
        if self.openai_settings:
            return AiConnectorType.OPEN_AI
        if self.azure_openai_settings:
            return AiConnectorType.AZURE_OPEN_AI
        if self.ollama_settings:
            return AiConnectorType.OLLAMA
        if self.embedded_settings:
            return AiConnectorType.EMBEDDED
        if self.google_settings:
            return AiConnectorType.GOOGLE
        if self.huggingface_settings:
            return AiConnectorType.HUGGING_FACE
        if self.mistral_ai_settings:
            return AiConnectorType.MISTRAL_AI
        if self.vertex_settings:
            return AiConnectorType.VERTEX
        return AiConnectorType.NONE

    def using_encrypted_communication_channel(self) -> bool:
        """Returns True if the connection uses HTTPS (encrypted communication)."""
        active_settings = None
        if self.openai_settings:
            active_settings = self.openai_settings
        elif self.azure_openai_settings:
            active_settings = self.azure_openai_settings
        elif self.ollama_settings:
            active_settings = self.ollama_settings
        elif self.google_settings:
            active_settings = self.google_settings
        elif self.huggingface_settings:
            active_settings = self.huggingface_settings
        elif self.mistral_ai_settings:
            active_settings = self.mistral_ai_settings
        elif self.vertex_settings:
            active_settings = self.vertex_settings

        if active_settings and hasattr(active_settings, "endpoint") and active_settings.endpoint:
            return active_settings.endpoint.lower().startswith("https://")

        # Embedded settings don't have an endpoint
        return False

    def to_json(self) -> Dict[str, Any]:
        return {
            "Name": self.name,
            "Identifier": self.identifier,
            "OpenAiSettings": self.openai_settings.to_json() if self.openai_settings else None,
            "AzureOpenAiSettings": self.azure_openai_settings.to_json() if self.azure_openai_settings else None,
            "OllamaSettings": self.ollama_settings.to_json() if self.ollama_settings else None,
            "EmbeddedSettings": self.embedded_settings.to_json() if self.embedded_settings else None,
            "GoogleSettings": self.google_settings.to_json() if self.google_settings else None,
            "HuggingFaceSettings": self.huggingface_settings.to_json() if self.huggingface_settings else None,
            "MistralAiSettings": self.mistral_ai_settings.to_json() if self.mistral_ai_settings else None,
            "VertexSettings": self.vertex_settings.to_json() if self.vertex_settings else None,
            "ModelType": self.model_type.value if self.model_type else None,
            "Type": self.get_type,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "AiConnectionString":
        return cls(
            name=json_dict["Name"],
            identifier=json_dict["Identifier"],
            openai_settings=(
                OpenAiSettings.from_json(json_dict["OpenAiSettings"]) if json_dict.get("OpenAiSettings") else None
            ),
            azure_openai_settings=(
                AzureOpenAiSettings.from_json(json_dict["AzureOpenAiSettings"])
                if json_dict.get("AzureOpenAiSettings")
                else None
            ),
            ollama_settings=(
                OllamaSettings.from_json(json_dict["OllamaSettings"]) if json_dict.get("OllamaSettings") else None
            ),
            embedded_settings=(
                EmbeddedSettings.from_json(json_dict["EmbeddedSettings"]) if json_dict.get("EmbeddedSettings") else None
            ),
            google_settings=(
                GoogleSettings.from_json(json_dict["GoogleSettings"]) if json_dict.get("GoogleSettings") else None
            ),
            huggingface_settings=(
                HuggingFaceSettings.from_json(json_dict["HuggingFaceSettings"])
                if json_dict.get("HuggingFaceSettings")
                else None
            ),
            mistral_ai_settings=(
                MistralAiSettings.from_json(json_dict["MistralAiSettings"])
                if json_dict.get("MistralAiSettings")
                else None
            ),
            vertex_settings=(
                VertexSettings.from_json(json_dict["VertexSettings"]) if json_dict.get("VertexSettings") else None
            ),
            model_type=AiModelType(json_dict["ModelType"]) if json_dict.get("ModelType") else None,
        )
