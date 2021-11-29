from typing import Optional

from pyravendb.documents.operations.backups import (
    LocalSettings,
    S3Settings,
    AzureSettings,
    GlacierSettings,
    GoogleCloudSettings,
    FtpSettings,
)
from pyravendb.documents.operations.connection_strings import ConnectionString
from pyravendb.documents.operations.etl import EtlConfiguration
from pyravendb.serverwide import ConnectionStringType


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
        return ConnectionStringType.OLAP


# todo: implement
class OlapEtlConfiguration(EtlConfiguration[OlapConnectionString]):
    pass