from enum import Enum
from typing import Dict, Any

from ravendb.documents.operations.ai.abstract_ai_settings import AbstractAiSettings


class GoogleAiVersion(Enum):
    V1 = "V1"
    V1_Beta = "V1_Beta"


class GoogleSettings(AbstractAiSettings):
    def __init__(
        self,
        model: str = None,
        api_key: str = None,
        ai_version: GoogleAiVersion = None,
        dimensions: int = None,
        embeddings_max_concurrent_batches: int = None,
    ):
        super().__init__(embeddings_max_concurrent_batches)
        self.model = model
        self.api_key = api_key
        self.ai_version = ai_version
        self.dimensions = dimensions

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "GoogleSettings":
        return cls(
            model=json_dict["Model"] if "Model" in json_dict else None,
            api_key=json_dict["ApiKey"] if "ApiKey" in json_dict else None,
            ai_version=GoogleAiVersion(json_dict["AiVersion"]) if "AiVersion" in json_dict else None,
            dimensions=json_dict["Dimensions"] if "Dimensions" in json_dict else None,
            embeddings_max_concurrent_batches=(
                json_dict["EmbeddingsMaxConcurrentBatches"] if "EmbeddingsMaxConcurrentBatches" in json_dict else None
            ),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Model": self.model,
            "ApiKey": self.api_key,
            "AiVersion": self.ai_version.value,
            "Dimensions": self.dimensions,
            "EmbeddingsMaxConcurrentBatches": self.embeddings_max_concurrent_batches,
        }
