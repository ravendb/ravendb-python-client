from typing import Dict, Any

from ravendb.documents.operations.ai.open_ai_base_settings import OpenAiBaseSettings


class OpenAiSettings(OpenAiBaseSettings):
    def __init__(
        self,
        api_key: str = None,
        endpoint: str = None,
        model: str = None,
        organization_id: str = None,
        project_id: str = None,
        dimensions: int = None,
        temperature: float = None,
    ):
        super().__init__(api_key, endpoint, model, dimensions, temperature)
        self.organization_id = organization_id
        self.project_id = project_id

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "OpenAiSettings":
        return cls(
            api_key=json_dict["ApiKey"],
            endpoint=json_dict["Endpoint"],
            model=json_dict["Model"],
            dimensions=json_dict["Dimensions"],
            temperature=json_dict["Temperature"],
            organization_id=json_dict["OrganizationId"],
            project_id=json_dict["ProjectId"],
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "ApiKey": self.api_key,
            "Endpoint": self.endpoint,
            "Model": self.model,
            "Dimensions": self.dimensions,
            "Temperature": self.temperature,
            "OrganizationId": self.organization_id,
            "ProjectId": self.project_id,
            "EmbeddingsMaxConcurrentBatches": self.embeddings_max_concurrent_batches,
        }
