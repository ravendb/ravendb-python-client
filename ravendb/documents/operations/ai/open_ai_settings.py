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
        embeddings_max_concurrent_batches: int = None,
        reasoning_effort: str = None,
    ):
        super().__init__(api_key, endpoint, model, dimensions, temperature, embeddings_max_concurrent_batches)
        self.organization_id = organization_id
        self.project_id = project_id
        # The reasoning_effort sent to the provider, controlling the reasoning depth of supported models
        # (such as the GPT-5 family). Typical values are "none", "minimal", "low", "medium", "high", "xhigh"
        # and "max"; model families accept different subsets. Sent as supplied apart from trimming and not
        # validated, so new provider values can be used directly. OpenAI accepts lowercase values only.
        # When not set the field is omitted and the model applies its own default.
        self.reasoning_effort = reasoning_effort

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "OpenAiSettings":
        return cls(
            api_key=json_dict["ApiKey"],
            endpoint=json_dict["Endpoint"],
            model=json_dict["Model"],
            dimensions=json_dict["Dimensions"] if "Dimensions" in json_dict else None,
            temperature=json_dict["Temperature"] if "Temperature" in json_dict else None,
            organization_id=json_dict["OrganizationId"] if "OrganizationId" in json_dict else None,
            project_id=json_dict["ProjectId"] if "ProjectId" in json_dict else None,
            embeddings_max_concurrent_batches=(
                json_dict["EmbeddingsMaxConcurrentBatches"] if "EmbeddingsMaxConcurrentBatches" in json_dict else None
            ),
            reasoning_effort=(
                json_dict["ReasoningEffort"] if isinstance(json_dict.get("ReasoningEffort"), str) else None
            ),
        )

    def to_json(self) -> Dict[str, Any]:
        json_dict = {
            "ApiKey": self.api_key,
            "Endpoint": self.endpoint,
            "Model": self.model,
            "Dimensions": self.dimensions,
            "Temperature": self.temperature,
            "OrganizationId": self.organization_id,
            "ProjectId": self.project_id,
            "EmbeddingsMaxConcurrentBatches": self.embeddings_max_concurrent_batches,
        }
        if self.reasoning_effort is not None and self.reasoning_effort.strip():
            json_dict["ReasoningEffort"] = self.reasoning_effort
        return json_dict
