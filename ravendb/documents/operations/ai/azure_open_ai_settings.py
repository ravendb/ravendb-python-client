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
            api_key=json_dict["ApiKey"] if "ApiKey" in json_dict else None,
            endpoint=json_dict["Endpoint"] if "Endpoint" in json_dict else None,
            model=json_dict["Model"] if "Model" in json_dict else None,
            dimensions=json_dict["Dimensions"] if "Dimensions" in json_dict else None,
            temperature=json_dict["Temperature"] if "Temperature" in json_dict else None,
            deployment_name=json_dict["DeploymentName"] if "DeploymentName" in json_dict else None,
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "ApiKey": self.api_key,
            "Endpoint": self.endpoint,
            "Model": self.model,
            "Dimensions": self.dimensions,
            "Temperature": self.temperature,
            "DeploymentName": self.deployment_name,
        }
