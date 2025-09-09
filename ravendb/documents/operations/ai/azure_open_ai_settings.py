from typing import Dict, Any

from ravendb.documents.operations.ai.open_ai_base_settings import OpenAiBaseSettings


class AzureOpenAiSettings(OpenAiBaseSettings):
    def __init__(
        self,
        api_key: str = None,
        endpoint: str = None,
        model: str = None,
        deployment_name: str = None,
        dimensions: int = None,
        temperature: float = None,
    ):
        super().__init__(api_key, endpoint, model, dimensions, temperature)
        self.deployment_name = deployment_name

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "AzureOpenAiSettings":
        return cls(
            api_key=json_dict["ApiKey"],
            endpoint=json_dict["Endpoint"],
            model=json_dict["Model"],
            dimensions=json_dict["Dimensions"],
            temperature=json_dict["Temperature"],
            deployment_name=json_dict["DeploymentName"],
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "ApiKey": self.api_key,
            "Endpoint": self.endpoint,
            "Model": self.model,
            "Dimensions": self.dimensions,
            "Temperature": self.temperature,
            "DeploymentName": self.deployment_name,
            "EmbeddingsMaxConcurrentBatches": self.embeddings_max_concurrent_batches,
        }
