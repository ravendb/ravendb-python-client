from __future__ import annotations
from abc import ABC
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any

from ravendb.tools.utils import Utils


class BackupType(Enum):
    BACKUP = "Backup"
    SNAPSHOT = "Snapshot"


class CompressionLevel(Enum):
    OPTIMAL = "Optimal"
    FASTEST = "Fastest"
    NO_COMPRESSION = "NoCompression"


class EncryptionMode(Enum):
    NONE = "None"
    USE_DATABASE_KEY = "UseDatabaseKey"
    USE_PROVIDED_KEY = "UseProvidedKey"


class GetBackupConfigurationScript:
    def __init__(self, exec: str = None, arguments: str = None, timeout_in_ms: int = None):
        self.exec = exec
        self.arguments = arguments
        self.timeout_in_ms = timeout_in_ms

    @classmethod
    def default(cls) -> GetBackupConfigurationScript:
        return cls(timeout_in_ms=10000)

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> GetBackupConfigurationScript:
        return cls(json_dict["Exec"], json_dict["Arguments"], json_dict["TimeoutInMs"])

    def to_json(self) -> Dict[str, Any]:
        return {"Exec": self.exec, "Arguments": self.arguments, "TimeoutInMs": self.timeout_in_ms}


class BackupSettings(ABC):
    def __init__(self, disabled: bool = None, get_backup_configuration_script: GetBackupConfigurationScript = None):
        self.disabled = disabled
        self.get_backup_configuration_script = get_backup_configuration_script

    def to_json(self) -> Dict[str, Any]:
        return {
            "Disabled": self.disabled,
            "GetBackupConfigurationScript": self.get_backup_configuration_script.to_json(),
        }


class LocalSettings(BackupSettings):
    def __init__(
        self,
        disabled: bool = None,
        get_backup_configuration_script: GetBackupConfigurationScript = None,
        folder_path: str = None,
    ):
        super().__init__(disabled, get_backup_configuration_script)
        self.folder_path = folder_path

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> LocalSettings:
        return cls(
            json_dict["Disabled"],
            GetBackupConfigurationScript.from_json(json_dict["GetBackupConfigurationScript"]),
            json_dict["FolderPath"],
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Disabled": self.disabled,
            "GetBackupConfigurationScript": self.get_backup_configuration_script.to_json(),
            "FolderPath": self.folder_path,
        }


class AmazonSettings(BackupSettings, ABC):
    def __init__(
        self,
        disabled: bool = None,
        get_backup_configuration_script: GetBackupConfigurationScript = None,
        aws_access_key: str = None,
        aws_secret_key: str = None,
        aws_session_token: str = None,
        aws_region_name: str = None,
        remote_folder_name: str = None,
    ):
        super().__init__(disabled, get_backup_configuration_script)
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.aws_session_token = aws_session_token
        self.aws_region_name = aws_region_name
        self.remote_folder_name = remote_folder_name


class S3Settings(AmazonSettings):
    def __init__(
        self,
        disabled: bool = None,
        get_backup_configuration_script: GetBackupConfigurationScript = None,
        aws_access_key: str = None,
        aws_secret_key: str = None,
        aws_session_token: str = None,
        aws_region_name: str = None,
        remote_folder_name: str = None,
        bucket_name: str = None,
        custom_server_url: str = None,
        force_path_style: bool = None,
    ):
        super().__init__(
            disabled,
            get_backup_configuration_script,
            aws_access_key,
            aws_secret_key,
            aws_session_token,
            aws_region_name,
            remote_folder_name,
        )
        self.bucket_name = bucket_name
        self.custom_server_url = custom_server_url
        self.force_path_style = force_path_style

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> S3Settings:
        return cls(
            json_dict["Disabled"],
            GetBackupConfigurationScript.from_json(json_dict["GetBackupConfigurationScript"]),
            json_dict["AwsAccessKey"],
            json_dict["AwsSecretKey"],
            json_dict["AwsSessionToken"],
            json_dict["AwsRegionName"],
            json_dict["RemoteFolderName"],
            json_dict["BucketName"],
            json_dict["CustomServerUrl"],
            json_dict["ForcePathStyle"],
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Disabled": self.disabled,
            "GetBackupConfigurationScript": self.get_backup_configuration_script.to_json(),
            "AwsAccessKey": self.aws_access_key,
            "AwsSecretKey": self.aws_secret_key,
            "AwsSessionToken": self.aws_session_token,
            "AwsRegionName": self.aws_region_name,
            "RemoteFolderName": self.remote_folder_name,
            "BucketName": self.bucket_name,
            "CustomServerUrl": self.custom_server_url,
            "ForcePathStyle": self.force_path_style,
        }


class GlacierSettings(AmazonSettings):
    def __init__(
        self,
        disabled: bool = None,
        get_backup_configuration_script: GetBackupConfigurationScript = None,
        aws_access_key: str = None,
        aws_secret_key: str = None,
        aws_session_token: str = None,
        aws_region_name: str = None,
        remote_folder_name: str = None,
        vault_name: str = None,
    ):
        super().__init__(
            disabled,
            get_backup_configuration_script,
            aws_access_key,
            aws_secret_key,
            aws_session_token,
            aws_region_name,
            remote_folder_name,
        )
        self.vault_name = vault_name

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> GlacierSettings:
        return cls(
            json_dict["Disabled"],
            GetBackupConfigurationScript.from_json(json_dict["GetBackupConfigurationScript"]),
            json_dict["AwsAccessKey"],
            json_dict["AwsSecretKey"],
            json_dict["AwsSessionToken"],
            json_dict["AwsRegionName"],
            json_dict["RemoteFolderName"],
            json_dict["VaultName"],
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Disabled": self.disabled,
            "GetBackupConfigurationScript": self.get_backup_configuration_script.to_json(),
            "AwsAccessKey": self.aws_access_key,
            "AwsSecretKey": self.aws_secret_key,
            "AwsSessionToken": self.aws_session_token,
            "AwsRegionName": self.aws_region_name,
            "RemoteFolderName": self.remote_folder_name,
            "VaultName": self.vault_name,
        }


class AzureSettings(BackupSettings):
    def __init__(
        self,
        disabled: bool = None,
        get_backup_configuration_script: GetBackupConfigurationScript = None,
        storage_container: str = None,
        remote_folder_name: str = None,
        account_name: str = None,
        account_key: str = None,
        sas_token: str = None,
    ):
        super().__init__(disabled, get_backup_configuration_script)
        self.storage_container = storage_container
        self.remote_folder_name = remote_folder_name
        self.account_name = account_name
        self.account_key = account_key
        self.sas_token = sas_token

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AzureSettings:
        return cls(
            json_dict["Disabled"],
            (
                GetBackupConfigurationScript.from_json(json_dict["GetBackupConfigurationScript"])
                if json_dict["GetBackupConfigurationScript"]
                else None
            ),
            json_dict["StorageContainer"],
            json_dict["RemoteFolderName"],
            json_dict["AccountName"],
            json_dict["AccountKey"],
            json_dict["SasToken"],
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Disabled": self.disabled,
            "GetBackupConfigurationScript": (
                self.get_backup_configuration_script.to_json() if self.get_backup_configuration_script else None
            ),
            "StorageContainer": self.storage_container,
            "RemoteFolderName": self.remote_folder_name,
            "AccountName": self.account_name,
            "AccountKey": self.account_key,
            "SasToken": self.sas_token,
        }


class FtpSettings(BackupSettings):
    def __init__(
        self,
        disabled: bool = None,
        get_backup_configuration_script: GetBackupConfigurationScript = None,
        url: str = None,
        port: int = None,
        user_name: str = None,
        password: str = None,
        certificate_as_base64: str = None,
        certificate_file_name: str = None,
    ):
        super().__init__(disabled, get_backup_configuration_script)
        self.url = url
        self.port = port
        self.user_name = user_name
        self.password = password
        self.certificate_as_base64 = certificate_as_base64
        self.certificate_file_name = certificate_file_name

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> FtpSettings:
        return cls(
            json_dict["Disabled"],
            (
                GetBackupConfigurationScript.from_json(json_dict["GetBackupConfigurationScript"])
                if json_dict["GetBackupConfigurationScript"]
                else None
            ),
            json_dict["Url"],
            json_dict["Port"] if "Port" in json_dict else None,
            json_dict["UserName"],
            json_dict["Password"],
            json_dict["CertificateAsBase64"],
            json_dict["CertificateFileName"] if "CertificateFileName" in json_dict else None,
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Disabled": self.disabled,
            "GetBackupConfigurationScript": (
                self.get_backup_configuration_script.to_json() if self.get_backup_configuration_script else None
            ),
            "Url": self.url,
            "Port": self.port,
            "UserName": self.user_name,
            "Password": self.password,
            "CertificateAsBase64": self.certificate_as_base64,
            "CertificateFileName": self.certificate_file_name,
        }


class BackupStatus(ABC):
    def __init__(
        self,
        last_full_backup: datetime = None,
        last_incremental_backup: datetime = None,
        full_backup_duration_in_ms: int = None,
        incremental_backup_duration_in_ms: int = None,
        exception: str = None,
    ):
        self.last_full_backup = last_full_backup
        self.last_incremental_backup = last_incremental_backup
        self.full_backup_duration_in_ms = full_backup_duration_in_ms
        self.incremental_backup_duration_in_ms = incremental_backup_duration_in_ms
        self.exception = exception


class GoogleCloudSettings(BackupStatus):
    def __init__(
        self,
        last_full_backup: datetime = None,
        last_incremental_backup: datetime = None,
        full_backup_duration_in_ms: int = None,
        incremental_backup_duration_in_ms: int = None,
        exception: str = None,
        bucket_name: str = None,
        remote_folder_name: str = None,
        google_credentials_json: str = None,
    ):
        super().__init__(
            last_full_backup,
            last_incremental_backup,
            full_backup_duration_in_ms,
            incremental_backup_duration_in_ms,
            exception,
        )
        self.bucket_name = bucket_name
        self.remote_folder_name = remote_folder_name
        self.google_credentials_json = google_credentials_json

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> GoogleCloudSettings:
        return cls(
            Utils.string_to_datetime(json_dict["LastFullBackup"]),
            Utils.string_to_datetime(json_dict["LastIncrementalBackup"]),
            json_dict["FullBackupDurationInMs"],
            json_dict["IncrementalBackupDurationInMs"],
            json_dict["Exception"],
            json_dict["BucketName"],
            json_dict["RemoteFolderName"],
            json_dict["GoogleCredentialsJson"],
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "LastFullBackup": Utils.datetime_to_string(self.last_full_backup),
            "LastIncrementalBackup": Utils.datetime_to_string(self.last_full_backup),
            "FullBackupDurationInMs": self.full_backup_duration_in_ms,
            "IncrementalBackupDurationInMs": self.incremental_backup_duration_in_ms,
            "Exception": self.exception,
            "BucketName": self.bucket_name,
            "RemoteFolderName": self.remote_folder_name,
            "GoogleCredentialsJson": self.google_credentials_json,
        }


class BackupEncryptionSettings:
    def __init__(self, key: str = None, encryption_mode: EncryptionMode = None):
        self.key = key
        self.encryption_mode = encryption_mode

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> BackupEncryptionSettings:
        return BackupEncryptionSettings(json_dict["Key"], EncryptionMode(json_dict["EncryptionMode"]))

    def to_json(self) -> Dict[str, Any]:
        return {"Key": self.key, "EncryptionMode": self.encryption_mode.value}


class SnapshotSettings:
    def __init__(self, compression_level: CompressionLevel = None):
        self.compression_level = compression_level

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> SnapshotSettings:
        return SnapshotSettings(CompressionLevel(json_dict["CompressionLevel"]))

    def to_json(self) -> Dict[str, Any]:
        return {"CompressionLevel": self.compression_level.value}


class BackupConfiguration:
    def __init__(
        self,
        backup_type: BackupType = None,
        snapshot_settings: SnapshotSettings = None,
        backup_encryption_settings: BackupEncryptionSettings = None,
        local_settings: LocalSettings = None,
        s3_settings: S3Settings = None,
        glacier_settings: GlacierSettings = None,
        azure_settings: AzureSettings = None,
        ftp_settings: FtpSettings = None,
        google_cloud_settings: GoogleCloudSettings = None,
    ):
        self.backup_type = backup_type
        self.snapshot_settings = snapshot_settings
        self.backup_encryption_settings = backup_encryption_settings
        self.local_settings = local_settings
        self.s3_settings = s3_settings
        self.glacier_settings = glacier_settings
        self.azure_settings = azure_settings
        self.ftp_settings = ftp_settings
        self.google_cloud_settings = google_cloud_settings

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> BackupConfiguration:
        return cls(
            BackupType(json_dict["BackupType"]),
            SnapshotSettings.from_json(json_dict["SnapshotSettings"]) if json_dict["SnapshotSettings"] else None,
            (
                BackupEncryptionSettings.from_json(json_dict["BackupEncryptionSettings"])
                if json_dict["BackupEncryptionSettings"]
                else None
            ),
            LocalSettings.from_json(json_dict["LocalSettings"]) if json_dict["LocalSettings"] else None,
            S3Settings.from_json(json_dict["S3Settings"]) if json_dict["S3Settings"] else None,
            GlacierSettings.from_json(json_dict["GlacierSettings"]) if json_dict["GlacierSettings"] else None,
            AzureSettings.from_json(json_dict["AzureSettings"]) if json_dict["AzureSettings"] else None,
            FtpSettings.from_json(json_dict["FtpSettings"]) if json_dict["FtpSettings"] else None,
            (
                GoogleCloudSettings.from_json(json_dict["GoogleCloudSettings"])
                if json_dict["GoogleCloudSettings"]
                else None
            ),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "BackupType": self.backup_type.value if self.backup_type else None,
            "SnapshotSettings": self.snapshot_settings.to_json() if self.snapshot_settings else None,
            "BackupEncryptionSettings": (
                self.backup_encryption_settings.to_json() if self.backup_encryption_settings else None
            ),
            "LocalSettings": self.local_settings.to_json() if self.local_settings else None,
            "S3Settings": self.s3_settings.to_json() if self.s3_settings else None,
            "GlacierSettings": self.glacier_settings.to_json() if self.glacier_settings else None,
            "AzureSettings": self.azure_settings.to_json() if self.azure_settings else None,
            "FtpSettings": self.ftp_settings.to_json() if self.ftp_settings else None,
            "GoogleCloudSettings": self.google_cloud_settings.to_json() if self.google_cloud_settings else None,
        }


class RetentionPolicy:
    def __init__(self, disabled: bool = None, minimum_backup_age_to_keep: timedelta = None):
        self.disabled = disabled
        self.minimum_backup_age_to_keep = minimum_backup_age_to_keep

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> RetentionPolicy:
        return cls(json_dict["Disabled"], Utils.string_to_timedelta(json_dict["MinimumBackupAgeToKeep"]))

    def to_json(self) -> Dict[str, Any]:
        return {
            "Disabled": self.disabled,
            "MinimumBackupAgeToKeep": Utils.timedelta_to_str(self.minimum_backup_age_to_keep),
        }


class PeriodicBackupConfiguration(BackupConfiguration):
    def __init__(
        self,
        backup_type: BackupType = None,
        snapshot_settings: SnapshotSettings = None,
        backup_encryption_settings: BackupEncryptionSettings = None,
        local_settings: LocalSettings = None,
        s3_settings: S3Settings = None,
        glacier_settings: GlacierSettings = None,
        azure_settings: AzureSettings = None,
        ftp_settings: FtpSettings = None,
        google_cloud_settings: GoogleCloudSettings = None,
        name: str = None,
        task_id: int = None,
        disabled: bool = None,
        mentor_node: str = None,
        pin_to_mentor_node: bool = None,
        retention_policy: RetentionPolicy = None,
        full_backup_frequency: str = None,
        incremental_backup_frequency: str = None,
    ):
        super().__init__(
            backup_type,
            snapshot_settings,
            backup_encryption_settings,
            local_settings,
            s3_settings,
            glacier_settings,
            azure_settings,
            ftp_settings,
            google_cloud_settings,
        )
        self.name = name
        self.task_id = task_id
        self.disabled = disabled
        self.mentor_node = mentor_node
        self.pin_to_mentor_node = pin_to_mentor_node
        self.retention_policy = retention_policy
        self.full_backup_frequency = full_backup_frequency
        self.incremental_backup_frequency = incremental_backup_frequency

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> PeriodicBackupConfiguration:
        backup_conf = BackupConfiguration.from_json(json_dict)
        return PeriodicBackupConfiguration(
            backup_conf.backup_type,
            backup_conf.snapshot_settings,
            backup_conf.backup_encryption_settings,
            backup_conf.local_settings,
            backup_conf.s3_settings,
            backup_conf.glacier_settings,
            backup_conf.azure_settings,
            backup_conf.ftp_settings,
            backup_conf.google_cloud_settings,
            json_dict["Name"],
            json_dict["TaskId"],
            json_dict["Disabled"],
            json_dict["MentorNode"],
            json_dict["PinToMentorNode"],
            RetentionPolicy.from_json(json_dict["RetentionPolicy"]) if json_dict["RetentionPolicy"] else None,
            json_dict["FullBackupFrequency"],
            json_dict["IncrementalBackupFrequency"],
        )

    def to_json(self) -> Dict[str, Any]:
        json_dict = super().to_json()
        json_dict.update(
            {
                "Name": self.name,
                "TaskId": self.task_id,
                "Disabled": self.disabled,
                "MentorNode": self.mentor_node,
                "PinToMentorNode": self.pin_to_mentor_node,
                "RetentionPolicy": self.retention_policy.to_json() if self.retention_policy else None,
                "FullBackupFrequency": self.full_backup_frequency,
                "IncrementalBackupFrequency": self.incremental_backup_frequency,
            }
        )

        return json_dict
