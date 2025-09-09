from typing import Optional, Dict, Any


class AmazonSqsCredentials:
    def __init__(
        self,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region_name: Optional[str] = None,
    ):
        self.access_key = access_key
        self.secret_key = secret_key
        self.region_name = region_name

    def to_json(self) -> Dict[str, Any]:
        return {
            "AccessKey": self.access_key,
            "SecretKey": self.secret_key,
            "RegionName": self.region_name,
        }

    @classmethod
    def from_json(cls, json_dict: Optional[Dict[str, Any]]):
        return cls(
            access_key=json_dict.get("AccessKey"),
            secret_key=json_dict.get("SecretKey"),
            region_name=json_dict.get("RegionName"),
        )


class AmazonSqsConnectionSettings:
    EMULATOR_URL_ENVIRONMENT_VARIABLE = "RAVEN_AMAZON_SQS_EMULATOR_URL"

    def __init__(
        self,
        basic: Optional[AmazonSqsCredentials] = None,
        passwordless: Optional[bool] = None,
        use_emulator: Optional[bool] = None,
    ):
        self.basic = basic
        self.passwordless = passwordless
        self.use_emulator = use_emulator

    def to_json(self) -> Dict[str, Any]:
        return {
            "Basic": self.basic.to_json() if self.basic else None,
            "Passwordless": self.passwordless,
            "UseEmulator": self.use_emulator,
        }

    @classmethod
    def from_json(cls, json_dict: Optional[Dict[str, Any]]):
        basic_dict = json_dict.get("Basic")
        return cls(
            basic=AmazonSqsCredentials.from_json(basic_dict) if basic_dict else None,
            passwordless=json_dict.get("Passwordless"),
            use_emulator=json_dict.get("UseEmulator"),
        )
