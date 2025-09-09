import enum
from typing import Optional, Dict, Any

import ravendb.serverwide.server_operation_executor
from ravendb.documents.operations.ai.azure_open_ai_settings import AzureOpenAiSettings
from ravendb.documents.operations.ai.embedded_settings import EmbeddedSettings
from ravendb.documents.operations.ai.google_settings import GoogleSettings
from ravendb.documents.operations.ai.hugging_face_settings import HuggingFaceSettings
from ravendb.documents.operations.ai.mistral_ai_settings import MistralAiSettings
from ravendb.documents.operations.ai.ollama_settings import OllamaSettings
from ravendb.documents.operations.ai.open_ai_settings import OpenAiSettings
from ravendb.documents.operations.connection_strings import ConnectionString


class AiModelType(enum.Enum):
    TEXT_EMBEDDINGS = "TextEmbeddings",
    CHAT = "Chat"


class AiConnectionString(ConnectionString): # todo kuba
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
        model_type: AiModelType = None
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

    @property
    def get_type(self):
        return ravendb.serverwide.server_operation_executor.ConnectionStringType.AI.value

    def to_json(self) -> Dict[str, Any]:
        return {
            "Name": self.name,
            "Identifier": self.identifier,
            "OpenaiSettings": self.openai_settings,
            "AzureOpenaiSettings": self.azure_openai_settings,
            "ollama_settings": self.ollama_settings,
            "EmbeddedSettings": self.embedded_settings,
            "GoogleSettings": self.google_settings,
            "HuggingfaceSettings": self.huggingface_settings,
            "MistralAiSettings": self.mistral_ai_settings,
            "ModelType": self.model_type,
            "Type": ravendb.serverwide.server_operation_executor.ConnectionStringType.OLAP,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "AiConnectionString":
        return cls(
            name=json_dict["Name"],
            identifier=json_dict["Identifier"],
            openai_settings=OpenAiSettings.from_json(json_dict["OpenaiSettings"]),
            azure_openai_settings=AzureOpenAiSettings.from_json(json_dict["AzureOpenaiSettings"]),
            ollama_settings=OllamaSettings.from_json(json_dict["ollama_settings"]),
            embedded_settings=EmbeddedSettings.from_json(json_dict["EmbeddedSettings"]),
            google_settings=GoogleSettings.from_json(json_dict["GoogleSettings"]),
            huggingface_settings=HuggingFaceSettings.from_json(json_dict["HuggingfaceSettings"]),
            mistral_ai_settings=MistralAiSettings.from_json(json_dict["MistralAiSettings"]),
            model_type=AiModelType(json_dict["ModelType"]) if json_dict["ModelType"] else None,
        )
