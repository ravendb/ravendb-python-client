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
from ravendb.documents.operations.connection_strings import ConnectionString


class AiModelType(enum.Enum):
    TEXT_EMBEDDINGS = "TextEmbeddings"
    CHAT = "Chat"


class AiConnectionString(ConnectionString):  # todo kuba
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
            ]
        ):
            raise ValueError(
                "Please provide at least one of the following settings: openai_settings, azure_openai_settings, ollama_settings, embedded_settings, google_settings, huggingface_settings, mistral_ai_settings"
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
        ]:
            if setting:
                settings_set_count += 1 if setting else 0
            if settings_set_count > 1:
                raise ValueError(
                    "Please provide only one of the following settings: openai_settings, azure_openai_settings, ollama_settings, embedded_settings, google_settings, huggingface_settings, mistral_ai_settings"
                )

    @property
    def get_type(self):
        return ConnectionStringType.AI.value

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
            model_type=AiModelType(json_dict["ModelType"]) if json_dict.get("ModelType") else None,
        )
