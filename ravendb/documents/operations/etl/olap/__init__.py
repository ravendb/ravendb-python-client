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
import ravendb.serverwide
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
        return ravendb.serverwide.ConnectionStringType.OLAP


# todo: implement
class OlapEtlConfiguration(EtlConfiguration[OlapConnectionString]):
    pass
