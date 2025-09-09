from typing import Optional

from ravendb.documents.operations.backups.settings import (
    LocalSettings,
    S3Settings,
    AzureSettings,
    GlacierSettings,
    GoogleCloudSettings,
    FtpSettings,
)
from ravendb.documents.operations.connection_strings import ConnectionString
import ravendb.serverwide.server_operation_executor
from ravendb.documents.operations.etl.configuration import EtlConfiguration


class OlapConnectionString(ConnectionString):
    def __init__(
        self,
        name: str,
        local_settings: Optional[LocalSettings] = None,
        s3_settings: Optional[S3Settings] = None,
        azure_settings: Optional[AzureSettings] = None,
        glacier_settings: Optional[GlacierSettings] = None,
        google_cloud_settings: Optional[GoogleCloudSettings] = None,
        ftp_settings: Optional[FtpSettings] = None,
    ):
        super().__init__(name)
        self.local_settings = local_settings
        self.s3_settings = s3_settings
        self.azure_settings = azure_settings
        self.glacier_settings = glacier_settings
        self.google_cloud_settings = google_cloud_settings
        self.ftp_settings = ftp_settings

    @property
    def get_type(self):
        return ravendb.serverwide.server_operation_executor.ConnectionStringType.OLAP.value

    def to_json(self):
        return {
            "Name": self.name,
            "LocalSettings": self.local_settings,
            "S3Settings": self.s3_settings,
            "AzureSettings": self.azure_settings,
            "GlacierSettings": self.glacier_settings,
            "GoogleCloudSettings": self.google_cloud_settings,
            "FtpSettings": self.ftp_settings,
            "Type": ravendb.serverwide.server_operation_executor.ConnectionStringType.OLAP,
        }

    @classmethod
    def from_json(cls, json_dict: dict) -> "OlapConnectionString":
        return cls(
            name=json_dict["Name"],
            local_settings=LocalSettings.from_json(json_dict["LocalSettings"]) if json_dict["LocalSettings"] else None,
            s3_settings=S3Settings.from_json(json_dict["S3Settings"]) if json_dict["S3Settings"] else None,
            azure_settings=AzureSettings.from_json(json_dict["AzureSettings"]) if json_dict["AzureSettings"] else None,
            glacier_settings=(
                GlacierSettings.from_json(json_dict["GlacierSettings"]) if json_dict["GlacierSettings"] else None
            ),
            google_cloud_settings=(
                GoogleCloudSettings.from_json(json_dict["GoogleCloudSettings"])
                if json_dict["GoogleCloudSettings"]
                else None
            ),
            ftp_settings=FtpSettings.from_json(json_dict["FtpSettings"]) if json_dict["FtpSettings"] else None,
        )


# todo: implement
class OlapEtlConfiguration(EtlConfiguration[OlapConnectionString]):
    pass
