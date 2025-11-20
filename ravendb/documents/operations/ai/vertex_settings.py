from enum import Enum
from typing import Dict, Any, Optional

from ravendb.documents.operations.ai.abstract_ai_settings import AbstractAiSettings


class VertexAIVersion(Enum):
    V1 = "V1"
    V1_BETA = "V1_Beta"


class VertexSettings(AbstractAiSettings):
    def __init__(
        self,
        model: Optional[str] = None,
        google_credentials_json: Optional[str] = None,
        location: Optional[str] = None,
        ai_version: Optional[VertexAIVersion] = None,
    ):
        super().__init__()
        self.model = model
        self.google_credentials_json = google_credentials_json
        self.location = location
        self.ai_version = ai_version

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "VertexSettings":
        return cls(
            model=json_dict.get("Model"),
            google_credentials_json=json_dict.get("GoogleCredentialsJson"),
            location=json_dict.get("Location"),
            ai_version=VertexAIVersion(json_dict["AiVersion"]) if json_dict.get("AiVersion") else None,
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Model": self.model,
            "GoogleCredentialsJson": self.google_credentials_json,
            "AiVersion": self.ai_version.value if self.ai_version else None,
            "Location": self.location,
            "EmbeddingsMaxConcurrentBatches": self.embeddings_max_concurrent_batches,
        }
