from typing import Dict, Any

from ravendb.documents.operations.ai.abstract_ai_settings import AbstractAiSettings


class MistralAiSettings(AbstractAiSettings):
    def __init__(self, api_key: str = None, model: str = None, endpoint: str = None):
        super().__init__()
        self.api_key = api_key
        self.model = model
        self.endpoint = endpoint

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "MistralAiSettings":
        return cls(
            api_key=json_dict["ApiKey"],
            model=json_dict["Model"],
            endpoint=json_dict["Endpoint"],
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "ApiKey": self.api_key,
            "Model": self.model,
            "Endpoint": self.endpoint,
            "EmbeddingsMaxConcurrentBatches": self.embeddings_max_concurrent_batches,
        }
