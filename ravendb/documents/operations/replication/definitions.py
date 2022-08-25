# todo: implement
from datetime import datetime
from typing import Optional, List, Dict

from ravendb.tools.utils import Utils


class ReplicationNode:
    pass


class ExternalReplicationBase(ReplicationNode):
    pass


class ExternalReplication(ExternalReplicationBase):
    pass


class PullReplicationAsSink(ExternalReplicationBase):
    pass


class PullReplicationDefinition:
    pass


class DetailedReplicationHubAccess:
    def __init__(
        self,
        name: Optional[str] = None,
        thumbprint: Optional[str] = None,
        certificate: Optional[str] = None,
        not_before: Optional[datetime] = None,
        not_after: Optional[datetime] = None,
        subject: Optional[str] = None,
        issuer: Optional[str] = None,
        allowed_hub_to_sink_paths: Optional[List[str]] = None,
        allowed_sink_to_hub_paths: Optional[List[str]] = None,
    ):
        self.name = name
        self.thumbprint = thumbprint
        self.certificate = certificate
        self.not_before = not_before
        self.not_after = not_after
        self.subject = subject
        self.issuer = issuer
        self.allowed_hub_to_sink_paths = allowed_hub_to_sink_paths
        self.allowed_sink_to_hub_paths = allowed_sink_to_hub_paths

    def to_json(self) -> Dict:
        return {
            "Name": self.name,
            "Thumbprint": self.thumbprint,
            "Certificate": self.certificate,
            "NotBefore": Utils.datetime_to_string(self.not_before),
            "NotAfter": Utils.datetime_to_string(self.not_after),
            "Subject": self.subject,
            "Issuer": self.issuer,
            "AllowedHubToSinkPaths": self.allowed_hub_to_sink_paths,
            "AllowedSinkToHubPaths": self.allowed_sink_to_hub_paths,
        }
