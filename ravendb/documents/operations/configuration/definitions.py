from __future__ import annotations
from enum import Enum
from typing import Optional, Union

from ravendb.http.misc import ReadBalanceBehavior, LoadBalanceBehavior


class StudioEnvironment(Enum):
    NONE = "NONE"
    DEVELOPMENT = "DEVELOPMENT"
    TESTING = "TESTING"
    PRODUCTION = "PRODUCTION"


class StudioConfiguration:
    def __init__(self, disabled: Optional[bool] = None, environment: Optional[StudioEnvironment] = None):
        self.disabled = disabled
        self.environment = environment


class ClientConfiguration:
    def __init__(self):
        self.__identity_parts_separator: Union[None, str] = None
        self.etag: int = 0
        self.disabled: bool = False
        self.max_number_of_requests_per_session: Optional[int] = None
        self.read_balance_behavior: Optional[ReadBalanceBehavior] = None
        self.load_balance_behavior: Optional[LoadBalanceBehavior] = None
        self.load_balancer_context_seed: Optional[int] = None

    @property
    def identity_parts_separator(self) -> str:
        return self.__identity_parts_separator

    @identity_parts_separator.setter
    def identity_parts_separator(self, value: str):
        if value is not None and "|" == value:
            raise ValueError("Cannot set identity parts separator to '|'")
        self.__identity_parts_separator = value

    def to_json(self) -> dict:
        return {
            "IdentityPartsSeparator": self.__identity_parts_separator,
            "Etag": self.etag,
            "Disabled": self.disabled,
            "MaxNumberOfRequestsPerSession": self.max_number_of_requests_per_session,
            "ReadBalanceBehavior": (
                self.read_balance_behavior.value if self.read_balance_behavior else ReadBalanceBehavior.NONE
            ),
            "LoadBalanceBehavior": (
                self.load_balance_behavior.value if self.load_balance_behavior else LoadBalanceBehavior.NONE
            ),
            "LoadBalancerContextSeed": self.load_balancer_context_seed,
        }

    @classmethod
    def from_json(cls, json_dict: dict) -> Optional[ClientConfiguration]:
        if json_dict is None:
            return None
        config = cls()
        config.__identity_parts_separator = json_dict["IdentityPartsSeparator"]
        config.etag = json_dict["Etag"]
        config.disabled = json_dict["Disabled"]
        config.max_number_of_requests_per_session = json_dict["MaxNumberOfRequestsPerSession"]
        config.read_balance_behavior = ReadBalanceBehavior(json_dict["ReadBalanceBehavior"])
        config.load_balance_behavior = LoadBalanceBehavior(json_dict["LoadBalanceBehavior"])
        config.load_balancer_context_seed = json_dict["LoadBalancerContextSeed"]

        return config
