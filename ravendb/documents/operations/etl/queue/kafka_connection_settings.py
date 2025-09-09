from typing import Dict, Any


class KafkaConnectionSettings:
    def __init__(
        self,
        bootstrap_servers: str = None,
        connection_options: Dict[str, str] = None,
        use_raven_certificate: bool = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.connection_options = connection_options
        self.use_raven_certificate = use_raven_certificate

    def to_json(self):
        return {
            "BootstrapServers": self.bootstrap_servers,
            "ConnectionOptions": self.connection_options,
            "UseRavenCertificate": self.use_raven_certificate,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]):
        return cls(
            bootstrap_servers=json_dict.get("BootstrapServers"),
            connection_options=json_dict.get("ConnectionOptions"),
            use_raven_certificate=json_dict.get("UseRavenCertificate"),
        )
